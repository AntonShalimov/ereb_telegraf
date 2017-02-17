package ereb_telegraf

import (
	"net/http"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
	"strings"
	"encoding/json"
	"time"
	"sync"
	"net/url"
	"fmt"
	"strconv"
)

type ereb struct {
	Servers []string
	client *http.Client
}

type ErebStatus struct {
	NextRun   float64 `json:"next_run"`
	NextTasks []struct {
		Cmd            string        `json:"cmd"`
		CronSchedule   string        `json:"cron_schedule"`
		Description    string        `json:"description"`
		Enabled        bool          `json:"enabled"`
		Group          string        `json:"group"`
		Name           string        `json:"name"`
		ShellScripts   []interface{} `json:"shell_scripts"`
		TaskID         string        `json:"task_id"`
		Timeout        string        `json:"timeout"`
		TryMoreOnError bool          `json:"try_more_on_error"`
	} `json:"next_tasks"`
	PlannedTaskRunUuids []string `json:"planned_task_run_uuids"`
	State               string   `json:"state"`
}

type ErebTasks []struct {
	Cmd          string        `json:"cmd"`
	CronSchedule string        `json:"cron_schedule"`
	Description  string        `json:"description"`
	Enabled      bool          `json:"enabled"`
	Group        string        `json:"group"`
	Name         string        `json:"name"`
	ShellScripts []interface{} `json:"shell_scripts"`
	Stats        struct {
		DurationAvg float64  `json:"duration_avg"`
		DurationMax int64    `json:"duration_max"`
		DurationMin int64    `json:"duration_min"`
		Error       int64    `json:"error"`
		ExitCodes   []string `json:"exit_codes"`
		Success     int64    `json:"success"`
		TaskID      string   `json:"task_id"`
	} `json:"stats"`
	TaskID         string `json:"task_id"`
	Timeout        string `json:"timeout"`
	TryMoreOnError bool   `json:"try_more_on_error"`
}

type gatherFunc func(g *ereb, serverAddr string, acc telegraf.Accumulator) error
var gatherFunctions = []gatherFunc{gatherStatus, gatherTasks}

const sampleConfig = `
  ## An array of address to gather stats about.
  ## If no servers are specified, then default to 127.0.0.1:8888
  # servers = ["http://localhost:8888"]
`

func (g *ereb) SampleConfig() string {
	return sampleConfig
}

func (g *ereb) Description() string {
	return "Read task details from your ereb instance"
}


func (g *ereb) Gather(acc telegraf.Accumulator) error {
	if len(g.Servers) == 0 {
		g.Servers = append(g.Servers, "http://localhost:8888")
	}

	endpoints := make([]string, 0, len(g.Servers))

	for _, endpoint := range g.Servers {

		if strings.HasPrefix(endpoint, "http") {
			endpoints = append(endpoints, endpoint)
			continue
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(endpoints) * len(gatherFunctions))
	for _, server := range endpoints {
		for _, f := range gatherFunctions {
			go func(serv string, gf gatherFunc) {
				defer wg.Done()
				gf(g, server, acc)
			}(server, f)
		}
	}

	wg.Wait()
	return nil
}

func gatherStatus(g *ereb, serverAddr string, acc telegraf.Accumulator) error {
	erebStatus := &ErebStatus{}

	err := g.getJson(serverAddr + "/status", &erebStatus)
	if err != nil {
		return err
	}

	u, err := url.Parse(serverAddr)

	tags := map[string]string{"host": u.Host}

	now := time.Now()
	is_running := false
	if erebStatus.State == "running" {
		is_running = true
	}

	fields := map[string]interface{}{
		"running": is_running,
		"tasks_queue_length": len(erebStatus.NextTasks),
		"next_run_in": erebStatus.NextRun,
	}

	acc.AddFields("ereb_status", fields, tags, now)

	return err
}


func gatherTasks(g *ereb, serverAddr string, acc telegraf.Accumulator) error {
	now := time.Now()

	erebTasks := ErebTasks{}
	err := g.getJson(serverAddr + "/status", &erebTasks)
	if err != nil {
		return err
	}

	u, err := url.Parse(serverAddr)

	for _, task := range erebTasks {
		tags := map[string]string{
			"host": u.Host,
			"task_name": task.Name,
		}

		exitCodes := task.Stats.ExitCodes
		taskTimeout, _ := strconv.Atoi(task.Timeout)
		fields := map[string]interface{}{
			"enabled":        task.Enabled,
			"success_count":  task.Stats.Success,
			"errors_count":   task.Stats.Error,
			"avg_duration":   task.Stats.DurationAvg,
			"max_duration":   task.Stats.DurationMax,
			"min_duration":   task.Stats.DurationMin,
			"timeout":        taskTimeout,
			"last_exit_code": exitCodes[len(exitCodes) - 1],
		}

		acc.AddFields("ereb_tasks", fields, tags, now)
	}


	return err
}



func (g *ereb) getJson(requestUrl string, target interface{}) error {
	if g.client == nil {
		tr := &http.Transport{ResponseHeaderTimeout: time.Duration(3 * time.Second)}
		client := &http.Client{
			Transport: tr,
			Timeout:   time.Duration(4 * time.Second),
		}
		g.client = client
	}

	u, err := url.Parse(requestUrl)
	if err != nil {
		return fmt.Errorf("Unable parse server address '%s': %s", requestUrl, err)
	}

	req, err := http.NewRequest("GET", requestUrl, nil)
	if u.User != nil {
		p, _ := u.User.Password()
		req.SetBasicAuth(u.User.Username(), p)
	}

	res, err := g.client.Do(req)
	if err != nil {
		return fmt.Errorf("Unable to connect to ereb server '%s': %s", requestUrl, err)
	}

	if res.StatusCode != 200 {
		return fmt.Errorf("Unable to get valid stat result from '%s', http response code : %d", requestUrl, res.StatusCode)
	}

	defer res.Body.Close()

	json.NewDecoder(res.Body).Decode(target)

	return nil
}

func init() {
	inputs.Add("ereb", func() telegraf.Input {
		return &ereb{}
	})
}