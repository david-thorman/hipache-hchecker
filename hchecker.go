package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"
)

const VERSION = "0.2.4"

var (
	myId              string
	cache             *Cache
	dryRun            = false
	runningCheckers   = 0
	statusCheckInterval = 60
)

func addCheck(line string) {
	check, err := NewCheck(line)
	if err != nil {
		log.Println("Warning: got invalid data on the \"dead\" channel:", line)
		return
	}
	if check.BackendGroupLength <= 1 {
		// Add the check only if the frontend is scaled to several
		// backends (backend is part of a group)
		return
	}
	locked, ch := cache.LockBackend(check)
	if locked == false {
		return
	}
	// Set all the callbacks for the check. They will be called during
	// the PingUrl at different steps
	check.SetDeadCallback(func() bool {
		r := true
		msg := "Flagging dead"
		if dryRun == false {
			r = cache.MarkBackendDead(check)
		} else {
			msg += " (dry run)"
		}
		log.Println(check.BackendUrl, msg)
		return r
	})
	check.SetAliveCallback(func() bool {
		r := true
		msg := "Flagging alive"
		if dryRun == false {
			r = cache.MarkBackendAlive(check)
		} else {
			msg += " (dry run)"
		}
		log.Println(check.BackendUrl, msg)
		return r
	})
	check.SetCheckIfBreakCallback(func() bool {
		return cache.IsUnlockedBackend(check)
	})
	check.SetExitCallback(func() {
		runningCheckers -= 1
		cache.UnlockBackend(check)
	})
	// Check the URL at a regular interval
	go check.PingUrl(ch)
	runningCheckers += 1
	log.Println(check.BackendUrl, "Added check")
}

/*
 * Prints some stats on runtime
 */
func printStats(cache *Cache) {
	const step = 10 // 10 seconds
	count := 0
	for {
		if dryRun == false {
			// In dry run mode, we don't announce our presence
			cache.PingAlive()
		}
		time.Sleep(time.Duration(step) * time.Second)
		count += step
		if count >= statusCheckInterval {
			// statusCheckInterval seconds
			count = 0
			msg := "backend URLs are being tested"
			if dryRun == true {
				msg += " (dry run)"
			}
			msg += ","
			log.Println(runningCheckers, msg, "using", runtime.NumGoroutine(),
				"goroutines")
		}
	}
}

/*
 * Enables CPU profile
 */
func enableCPUProfile() {
	cwd, _ := os.Getwd()
	log.Printf("CPU profile will be written to \"%s/%s\"\n",
		cwd, "hchecker.prof")
	f, err := os.Create("hchecker.prof")
	if err != nil {
		log.Fatal("Cannot enable CPU profile:", err)
	}
	pprof.StartCPUProfile(f)
}

/*
 * Listens to signals
 */
func handleSignals() {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGINT)
	go func() {
		switch <-c {
		case syscall.SIGINT:
			pprof.StopCPUProfile()
			os.Exit(0)
		}
	}()
}

/*
 * Actively Look for backends
 * XXX: there are most likely race conditions... but is should be ok
 *      with a single server set up.
 */
func findBackends(cache *Cache) {
	for {
		log.Println("Looking for backends")
		frontends,err := cache.redisConn.Keys("frontend:*").List()
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
		for _,frontend := range frontends {
			log.Println("Found Frontend:", frontend)
			backends,err := cache.redisConn.Lrange(frontend, 0, -1).List()
			frontend_id := strings.Join(strings.Split(strings.TrimSpace(frontend), ":")[1:],":")
			if err != nil {
				log.Println(err.Error())
				os.Exit(1)
			}
			for idx,backend := range backends {
				log.Println("Found backend:", idx-1, backend)
				if ( idx != 0 ) {
					check := fmt.Sprintf("%s;%s;%d;%d",frontend_id,backend,idx-1,len(backends)-1);

					// Probably want to find a better way to do
					// this but it seems ok for now
					addCheck(check)
				}
			}
		}
		log.Println("Done looking for backends")
		time.Sleep(time.Duration(10) * time.Second)
	}
}

func parseFlags(cpuProfile *bool) {
	parseDuration := func(v *time.Duration, n string, def int, help string) {
		i := flag.Int(n, def, help)
		*v = time.Duration(*i) * time.Second
	}
	flag.StringVar(&httpMethod, "method", HTTP_METHOD,
		"HTTP method")
	flag.StringVar(&httpUri, "uri", HTTP_URI,
		"HTTP URI")
	flag.StringVar(&httpHost, "host", HTTP_HOST,
		"HTTP host header")
	parseDuration(&checkInterval, "interval", CHECK_INTERVAL,
		"Check interval (seconds)")
	parseDuration(&connectionTimeout, "connect", CONNECTION_TIMEOUT,
		"TCP connection timeout (seconds)")
	parseDuration(&ioTimeout, "io", IO_TIMEOUT,
		"Socket read/write timeout (seconds)")
    flag.IntVar(&statusCheckInterval, "status", 60,
        "Interval of reporting status (seconds)")
	flag.StringVar(&redisAddress, "redis", REDIS_ADDRESS,
		"Network address of Redis")
	flag.BoolVar(cpuProfile, "cpuprofile", false,
		"Write CPU profile to \"hchecker.prof\" (current directory)")
	flag.BoolVar(&dryRun, "dryrun", false,
		"Enable dry run (or simulation mode). Do not update the Redis.")
	flag.Parse()
}

func main() {
	var (
		err        error
		hostname   string
		cpuProfile bool
	)
	fmt.Println("hchecker version", VERSION)
	for _, arg := range os.Args {
		if !(arg == "-v" || arg == "--version" || arg == "-version") {
			continue
		}
		os.Exit(0)
	}
	parseFlags(&cpuProfile)
	if dryRun == true {
		fmt.Println("Enabled dry run mode (simulation)")
	}
	// Force 1 CPU to reduce parallelism. If you want to use more CPUs, prefer
	// spawning several processes instead.
	runtime.GOMAXPROCS(1)
	hostname, _ = os.Hostname()
	myId = fmt.Sprintf("%s#%d", hostname, os.Getpid())
	// Prefix each line of log
	log.SetPrefix(myId + " ")
	if cpuProfile == true {
		enableCPUProfile()
	}
	handleSignals()
	cache, err = NewCache()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	err = cache.ListenToChannel("dead", addCheck)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// This function will check redis for all frontends and add checks for each backend
	go findBackends(cache)

	// This function will block and print the stats every statusCheckInterval
	// seconds
	printStats(cache)
}
