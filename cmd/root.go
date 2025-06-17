package cmd

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"regexp"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/encoding/mvt"
	"github.com/paulmach/orb/geojson"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

type Tile struct {
	zoom_level  int
	tile_column int
	tile_row    int
	tile_data   []byte
}

type Metadata struct {
	name  string
	value string
}

type VectorLayer struct {
	Fields  map[string]string `json:"fields"`
	Id      string            `json:"id"`
	Minzoom int               `json:"minzoom"`
	Maxzoom int               `json:"maxzoom"`
}

type MetaJson struct {
	VectorLayers []VectorLayer `json:"vector_layers"`
}

/**
TODO:
	- Some additional flags/arguments to add - precision to specify the coordinate precision of geometries, tile extent (default to MapBox extent)
	- Clean up styles parsing
*/

var rootCmd = &cobra.Command{
	Use:   "tinytiles [mbtiles] [style.json]",
	Short: "TinyTiles is a CLI tool for minimizing MBTiles.",
	Long:  "TinyTiles is a CLI tool for minimizing MBTiles. It uses your style JSON to scan the MBTiles for unnecessary tile attributes and compresses the tiles using Gzip.",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		ProcessTiles(args)
	}}

var attributes []string

var sourceLayers []string

var keepAttributes string

var keepLayers string

var gzipped bool

var outputFile string

var removedAttributesMap map[string]bool

var removedLayersMap map[string]bool

func Execute() {
	rootCmd.Flags().StringVarP(&keepAttributes, "keep-attributes", "a", "", "Write some Regex to specify which attributes should be kept no matter what.")
	rootCmd.Flags().StringVarP(&keepLayers, "keep-layers", "l", "", "Write some Regex to specify which layers should be kept no matter what.")
	rootCmd.Flags().StringVarP(&outputFile, "output", "o", "output.mbtiles", "The output file.")
	rootCmd.Flags().BoolVarP(&gzipped, "gzipped", "g", false, "The input data is gzipped.")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Oops. An error while executing Zero '%s'\n", err)
		os.Exit(1)
	}
}

func ProcessTiles(args []string) {

	// Parse style.json and find all attributes and layers in use
	fmt.Println("Scanning style.json for attributes and layers in use...")

	styleJson, err := os.Open(args[1])

	if err != nil {
		log.Fatal(err)
	}

	defer styleJson.Close()

	byteValue, _ := io.ReadAll(styleJson)

	var styles Style

	json.Unmarshal(byteValue, &styles)

	// Collect the attributes and layers in use
	for _, layer := range styles.Layers {
		sourceLayers = append(sourceLayers, layer.SourceLayer)
		attributes = append(attributes, getFields(layer.Filter)...)
	}

	// Unique the list of attributes
	attributes = uniqueSlice(attributes)

	fmt.Println("Processing tiles...")

	// Open input MBTiles
	db, err := sql.Open("sqlite3", args[0])

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	// Delete old output if it exists
	err = deleteFileIfExists(outputFile)
	if err != nil {
		log.Fatal(err)
	}

	// Copy input file to preserve all sqlite settings such as PAGE_SIZE
	copyFile(args[0], outputFile)

	// Open output MBTiles
	outputDb, err := sql.Open("sqlite3", outputFile)

	if err != nil {
		log.Fatal(err)
	}

	defer outputDb.Close()

	// Truncate metadata and tiles tables in new file
	_, err = outputDb.Exec("DELETE FROM metadata")
	if err != nil {
		log.Fatal(err)
	}

	_, err = outputDb.Exec("DELETE FROM tiles")
	if err != nil {
		log.Fatal(err)
	}

	// Clean up DB
	_, err = outputDb.Exec("VACUUM")
	if err != nil {
		log.Fatal(err)
	}

	// Transfer metadata from source MBTiles
	rows, err := db.Query("SELECT * FROM metadata")

	if err != nil {
		log.Fatal(err)
	}

	for rows.Next() {
		i := Metadata{}

		err = rows.Scan(&i.name, &i.value)
		if err != nil {
			log.Fatal(err)
		}

		if i.name == "json" {
			// Update the JSON meta field to reflect updated attributes (fields) and layers
			var metaJson MetaJson
			json.Unmarshal([]byte(i.value), &metaJson)

			var newVectorLayers []VectorLayer
			for _, layer := range metaJson.VectorLayers {

				if !containsSourceLayer(layer.Id) {
					continue
				}

				for field := range layer.Fields {
					if !containsAttr(field) {
						delete(layer.Fields, field)
					}
				}

				newVectorLayers = append(newVectorLayers, layer)
			}

			metaJson.VectorLayers = newVectorLayers

			metaJsonString, err := json.Marshal(metaJson)

			if err != nil {
				log.Fatal(err)
			}

			i.value = string(metaJsonString)
		}

		stmt, err := outputDb.Prepare("INSERT INTO metadata (name, value) VALUES(?, ?)")
		if err != nil {
			log.Fatal(err)
		}

		defer stmt.Close()

		_, err = stmt.Exec(i.name, i.value)
		if err != nil {
			log.Fatal(err)
		}
	}

	// Initialize maps
	removedLayersMap = make(map[string]bool)
	removedAttributesMap = make(map[string]bool)

	// Begin processing tiles
	totalTiles := getTotalTiles(db)

	bar := progressbar.Default(int64(totalTiles))

	chunks := math.Ceil(float64(totalTiles) / 100)

	c := 0
	for c < int(chunks) {

		offset := c * 100

		query := fmt.Sprintf("SELECT * FROM tiles ORDER BY zoom_level, tile_column, tile_row LIMIT 100 OFFSET %d", offset)

		// Fetch tiles from existing MBTiles
		rows, err = db.Query(query)

		if err != nil {
			log.Fatal(err)
		}

		transaction, err := outputDb.Begin()

		if err != nil {
			log.Fatal(err)
		}

		defer rows.Close()

		for rows.Next() {
			i := Tile{}

			err = rows.Scan(&i.zoom_level, &i.tile_column, &i.tile_row, &i.tile_data)
			if err != nil {
				log.Fatal(err)
			}

			var layers mvt.Layers
			if gzipped {
				layers, err = mvt.UnmarshalGzipped(i.tile_data)
			} else {
				layers, err = mvt.Unmarshal(i.tile_data)
			}

			if err != nil {
				log.Fatal(err)
			}

			var newLayers mvt.Layers

			for _, layer := range layers {
				// If the source layer is not in the style.json, don't add it to the new DB
				if !containsSourceLayer(layer.Name) {
					if !removedLayersMap[layer.Name] {
						removedLayersMap[layer.Name] = true
					}
					continue
				}

				newLayer := processFeatures(layer)

				newLayers = append(newLayers, newLayer)
			}

			newLayers.Clip(mvt.MapboxGLDefaultExtentBound)

			data, err := mvt.MarshalGzipped(newLayers)

			if err != nil {
				log.Fatal(err)
			}

			stmt, err := transaction.Prepare("INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES(?,?,?,?)")

			if err != nil {
				log.Fatal(err)
			}

			defer stmt.Close()

			_, err = stmt.Exec(i.zoom_level, i.tile_column, i.tile_row, data)

			if err != nil {
				log.Fatal(err)
			}

			bar.Add(1)
		}

		err = transaction.Commit()
		if err != nil {
			log.Fatal(err)
		}

		c++
	}

	// Clean up DB
	_, err = outputDb.Exec("VACUUM")
	if err != nil {
		log.Fatal(err)
	}

	// Pull file stats and calculate size reduction percentage
	originalFileInfo, err := os.Stat(args[0])

	if err != nil {
		log.Fatal(err)
	}

	originalFileSize := originalFileInfo.Size()

	outputFileInfo, err := os.Stat(outputFile)

	if err != nil {
		log.Fatal(err)
	}

	outputFileSize := outputFileInfo.Size()

	reducedByPercentage := (float32(originalFileSize-outputFileSize) / float32(originalFileSize)) * 100

	removedLayers := mapKeys(removedLayersMap)
	removedAttributes := mapKeys(removedAttributesMap)

	fmt.Printf("Removed layers: %s\n", strings.Join(removedLayers, ", "))
	fmt.Printf("Removed attributes: %s\n", strings.Join(removedAttributes, ", "))
	fmt.Printf("Input file size: %d bytes\n", originalFileSize)
	fmt.Printf("Output file size: %d bytes\n", outputFileSize)
	fmt.Printf("MBTiles reduced by %g%%\n", reducedByPercentage)
	fmt.Printf("File output at: %s", outputFile)
}

func processFeatures(layer *mvt.Layer) *mvt.Layer {
	features := geojson.NewFeatureCollection()

	for _, feature := range layer.Features {
		// Reduce precision of coordinates to 6 decimals
		newFeature := geojson.NewFeature(orb.Round(feature.Geometry))

		for property, value := range feature.Properties {
			if !containsAttr(property) {
				if !removedAttributesMap[property] {
					removedAttributesMap[property] = true
				}
				continue
			}

			newFeature.Properties[property] = value
		}

		features = features.Append(newFeature)
	}

	return mvt.NewLayer(layer.Name, features)
}

func containsSourceLayer(layerName string) bool {
	if keepLayers != "" {
		match, err := regexp.MatchString(keepLayers, layerName)

		if match {
			return true
		}

		if err != nil {
			log.Fatal(err)
		}
	}

	for _, value := range sourceLayers {
		if value == layerName {
			return true
		}
	}

	return false
}

func containsAttr(attribute string) bool {
	if strings.HasPrefix(attribute, "name") {
		return true
	}

	if strings.HasPrefix(attribute, "ref") {
		return true
	}

	if keepAttributes != "" {
		match, err := regexp.MatchString(keepAttributes, attribute)

		if match {
			return true
		}

		if err != nil {
			log.Fatal(err)
		}
	}

	for _, value := range attributes {
		if value == attribute {
			return true
		}
	}

	return false
}

func getTotalTiles(db *sql.DB) int {
	row := db.QueryRow("SELECT COUNT(*) FROM tiles")

	var total int

	err := row.Scan(&total)

	if err != nil {
		log.Fatal(err)
	}

	return total
}
