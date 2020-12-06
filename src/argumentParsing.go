package main

import (
	"flag"
)

type cmdlineArgs struct {
	rawHistogramPath        *string
	resultsOutputPath       *string
	fromAcceptableMissRatio *float64
	toAcceptableMissRatio   *float64
	stepAcceptableMissRatio *float64
	mrcPath                 *string
}

func parseArgs() *cmdlineArgs {
	cmdArgs := &cmdlineArgs{}
	cmdArgs.fromAcceptableMissRatio = flag.Float64("from", 0, "From acceptable miss ratio")
	cmdArgs.toAcceptableMissRatio = flag.Float64("to", 0, "To acceptable miss ratio")
	cmdArgs.stepAcceptableMissRatio = flag.Float64("step", 0, "Step acceptable miss ratio")
	cmdArgs.rawHistogramPath = flag.String("raw-data-path", ".", "The path to the directory containing the data from google")
	cmdArgs.resultsOutputPath = flag.String("result-output-path", ".", "The path to the directory to write the simulation results to")
	cmdArgs.mrcPath = flag.String("mrc-path", ".", "The path to the JSON file containing the MRC information")
	flag.Parse()
	return cmdArgs
}
