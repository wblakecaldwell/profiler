// Surface profiling information to a web client

package profiler

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// Commands HTTP endpoints send to the management goroutine
const startTracking = 1
const stopTracking = 2

// ExtraServiceInfoRetriever functions return a map with info about the running service.
type ExtraServiceInfoRetriever func() map[string]interface{}

var (
	// commands from outside are fed through here
	commandChannel chan int

	// proxy channel to handle requests from this channel
	proxyStatsRequestChannel chan chan []TimedMemStats

	// method we'll use to fetch extra, generic information from the running service
	extraServiceInfoRetriever      ExtraServiceInfoRetriever
	extraServiceInfoRetrieverMutex sync.RWMutex
)

// RegisterExtraServiceInfoRetriever sets the function that will provide us with extra service info when requested
func RegisterExtraServiceInfoRetriever(infoRetriever ExtraServiceInfoRetriever) {
	extraServiceInfoRetrieverMutex.Lock()
	extraServiceInfoRetrieverMutex.Unlock()

	extraServiceInfoRetriever = infoRetriever
}

func init() {
	// channel that this class uses to execute start/stop commands
	commandChannel = make(chan int)

	// channel we use to proxy memory stats requests through to the profiler if it's on, or to return empty results if not
	proxyStatsRequestChannel = make(chan chan []TimedMemStats)

	// management goroutine to handle memory profiling commands
	go func() {
		isTracking := false

		// when we're tracking memory, this is the channel we use to request the most recent memory statistics
		memStatsRequestChannel := make(chan chan []TimedMemStats)

		// when we're tracking memory, this is the quit channel for it - if we close it, memory profiling stops
		var memStatsQuitChannel chan bool

		for {
			// wait for commands
			select {
			case request := <-commandChannel:
				switch request {
				case startTracking:
					// someone wants to start tracking memory
					if !isTracking {
						log.Print("Starting to profile memory")

						// Keep 60 seconds of tracking data, recording 2 times per second
						memStatsQuitChannel = make(chan bool)
						TrackMemoryStatistics(60*2, 1000/2, memStatsRequestChannel, memStatsQuitChannel)

						isTracking = true
					}

				case stopTracking:
					// someone wants to stop tracking memory
					if isTracking {
						log.Print("Stopping profiling memory")
						close(memStatsQuitChannel)
						isTracking = false
					}
				}
			case responseChannel := <-proxyStatsRequestChannel:
				// handle a local request to get the memory stats that we've collected
				if !isTracking {
					// empty results
					responseChannel <- make([]TimedMemStats, 0)
				} else {
					// proxy results
					memStatsRequestChannel <- responseChannel
				}
			}
		}
	}()
}

// AddMemoryProfilingHandlers adds the memory profiling handlers
func AddMemoryProfilingHandlers(router *gin.Engine) {
	log.Println("AddMemoryProfilingHandlers")
	router.GET("/profiler/info.html", MemStatsHTMLHandler)
	router.GET("/profiler/info", ProfilingInfoJSONHandler)
	router.GET("/profiler/start", StartProfilingHandler)
	router.GET("/profiler/stop", StopProfilingHandler)
}

// StartProfiling is a function to start profiling automatically without web button
func StartProfiling() {
	commandChannel <- startTracking
}

// StopProfiling is a function to stop profiling automatically without web button
func StopProfiling() {
	commandChannel <- stopTracking
}

// StartProfilingHandler is a HTTP Handler to start memory profiling, if we're not already
func StartProfilingHandler(c *gin.Context) {
	log.Println("StartProfilingHandler")
	StartProfiling()
	time.Sleep(500 * time.Millisecond)
	c.Redirect(http.StatusTemporaryRedirect, "/profiler/info.html")
}

// StopProfilingHandler is a HTTP Handler to stop memory profiling, if we're profiling
func StopProfilingHandler(c *gin.Context) {
	log.Println("StopProfilingHandler")
	StopProfiling()
	time.Sleep(500 * time.Millisecond)
	c.Redirect(http.StatusTemporaryRedirect, "/profiler/info.html")
}

// ProfilingInfoJSONHandler is a HTTP Handler to return JSON of the Heap memory statistics and any extra info the server wants to tell us about
func ProfilingInfoJSONHandler(c *gin.Context) {
	log.Println("ProfilingInfoJSONHandler")
	// struct for output
	type outputStruct struct {
		HeapInfo         []HeapMemStat
		ExtraServiceInfo map[string]interface{}
	}
	response := outputStruct{}

	// Fetch the most recent memory statistics
	responseChannel := make(chan []TimedMemStats)
	proxyStatsRequestChannel <- responseChannel
	response.HeapInfo = timedMemStatsToHeapMemStats(<-responseChannel)

	// fetch the extra service info, if available
	extraServiceInfoRetrieverMutex.RLock()
	defer extraServiceInfoRetrieverMutex.RUnlock()
	if extraServiceInfoRetriever != nil {
		response.ExtraServiceInfo = extraServiceInfoRetriever()
	}

	// convert to JSON and write to the client
	js, err := json.Marshal(response)
	if err != nil {
		c.Error(err)
		return
	}
	//w.Write(js)
	c.Data(http.StatusOK, "application/json", js)
}

// MemStatsHTMLHandler is a HTTP Handler to fetch memstats.html or memstats-off.html content
func MemStatsHTMLHandler(c *gin.Context) {
	log.Println("MemStatsHTMLHandler")
	// Fetch the most recent memory statistics
	responseChannel := make(chan []TimedMemStats)

	// see if we have any data (this is temporary - eventually, JavaScript will see if there's data
	var response []TimedMemStats
	proxyStatsRequestChannel <- responseChannel
	response = <-responseChannel

	// fetch the template, or an error message if not available
	contentOrError := func(name string) string {
		contentBytes, err := Asset(name)
		content := string(contentBytes)
		if err != nil {
			content = err.Error()
		}
		return content
	}

	if len(response) == 0 {
		c.Writer.Write([]byte(contentOrError("info-off.html")))
		return
	}
	c.Writer.Write([]byte(contentOrError("info.html")))

}
