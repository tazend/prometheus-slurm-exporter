/* Copyright 2020 Victor Penso
   Copyright 2021 Toni Harzendorf

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
        "io/ioutil"
        "os/exec"
        "log"
        "strings"
        "strconv"
        "github.com/prometheus/client_golang/prometheus"
)

func QueueGetMetrics() map[string]*QueueMetrics {
	return ParseQueueMetrics(QueueData())
}

func QueueData() []byte {
        cmd := exec.Command("squeue","-a","-r", "-h", "--states=all", "-OPartition:.|,tres-alloc:.,State:.,UserName:.,Account:.,Reason:.")
        stdout, err := cmd.StdoutPipe()
        if err != nil {
                log.Fatal(err)
        }
        if err := cmd.Start(); err != nil {
                log.Fatal(err)
        }
        out, _ := ioutil.ReadAll(stdout)
        if err := cmd.Wait(); err != nil {
                log.Fatal(err)
        }
        return out
}

/* Pretty much always the same, might aswell use just one struct for all? */

type QueueCPUMetrics struct {
	allocated float64
	pending float64
}

type QueueMemoryMetrics struct {
	allocated float64
	pending float64
}

type QueueGPUMetrics struct {
	allocated float64
	pending float64
}

type QueueJobMetrics struct {
	running float64
	pending float64
	pending_dep float64
	suspended float64
	cancelled float64
	completing float64
	completed float64
	configuring float64
	failed float64
	timeout float64
	preempted float64
	node_fail float64
}

type UserMetrics struct {
	cpus QueueCPUMetrics
	memory QueueMemoryMetrics
	gpus QueueGPUMetrics
	jobs QueueJobMetrics
}

type AccountMetrics struct {
	users map[string]*UserMetrics
}

type QueueMetrics struct {
	accounts map[string]*AccountMetrics
}

func NewUserMetrics() *UserMetrics {

	/* Initialize Basic Empty Metrics */
	qcm := QueueCPUMetrics{0, 0}
	qmm := QueueMemoryMetrics{0, 0}
	qgm := QueueGPUMetrics{0, 0}
	qjm := QueueJobMetrics{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	user := &UserMetrics{
		qcm,
		qmm,
		qgm,
		qjm,
	}

	return user
}

func NewQueueMetrics() *QueueMetrics {

	queue := &QueueMetrics{
		make(map[string]*AccountMetrics),
	}

	return queue
}

func NewAccountMetrics() *AccountMetrics {

	account := &AccountMetrics{
		make(map[string]*UserMetrics),
	}

	return account
}

func ParseQueueMetrics(input []byte) map[string]*QueueMetrics {

	/* We will gather data about jobs for each partition */
        queue := make(map[string]*QueueMetrics)

	/* Loop through the job list */
        lines := strings.Split(string(input),"\n")
        for i, line := range lines {

		/* For some reason there is an empty line at the end. Why? */
		if len(lines)-1 == i {
			continue
		}

		/* Split input */
		split := strings.Split(line,"|")

		/* Assign splitted input */
		partition := split[0]
		tres_alloc := split[1]
		job_state := split[2]
		user := split[3]
		account := split[4]
		reason := split[5]

		/* Parse allocated TRES */
		tres_split := strings.Split(tres_alloc, ",")
		tres_cpu,_ := strconv.ParseFloat(strings.Split(tres_split[0], "=")[1], 64)
		tres_mem_str := strings.Split(tres_split[1], "=")[1]

		tres_mem := 0.0

		if strings.Contains(tres_mem_str, "G") {
			tres_mem,_ = strconv.ParseFloat(strings.Split(tres_mem_str,"G")[0], 64)
			tres_mem *= 1024.0
		} else if strings.Contains(tres_mem_str, "M") {
			tres_mem,_ = strconv.ParseFloat(strings.Split(tres_mem_str,"M")[0], 64)
		} else if strings.Contains(tres_mem_str, "T") {
			tres_mem,_ = strconv.ParseFloat(strings.Split(tres_mem_str,"T")[0], 64)
			tres_mem *= 1024.0 * 1024.0
		} else if strings.Contains(tres_mem_str, "K") {
			tres_mem,_ = strconv.ParseFloat(strings.Split(tres_mem_str,"K")[0], 64)
			tres_mem /= 1024.0
		}

		/* If partition has no entry yet, create one for the queue */
		_,pkey := queue[partition]
		if !pkey {
			queue[partition] = NewQueueMetrics()
		}

		/* If account doesn't exist yet, create it */
		_,akey := queue[partition].accounts[account]
		if !akey {
			queue[partition].accounts[account] = NewAccountMetrics()
		}

		/* If user has not entry yet, create one */
		_,ukey := queue[partition].accounts[account].users[user]
		if !ukey {
			queue[partition].accounts[account].users[user] = NewUserMetrics()
		}

		/* Fetch current User Metrics */
		mcpus := queue[partition].accounts[account].users[user].cpus
		mmemory := queue[partition].accounts[account].users[user].memory
		mgpus := queue[partition].accounts[account].users[user].gpus
		mjobs := queue[partition].accounts[account].users[user].jobs

		/* Change users Queue Metrics */
		switch job_state {
		case "RUNNING":
			mjobs.running += 1
			mcpus.allocated += tres_cpu
			mmemory.allocated += tres_mem

		case "PENDING":
			mjobs.pending += 1
			mcpus.pending += tres_cpu
			mmemory.pending += tres_mem

			if reason == "Dependency" {
				mjobs.pending_dep += 1
			}

		case "SUSPENDED":
			mjobs.suspended += 1

		case "CANCELLED":
			mjobs.cancelled += 1

		case "COMPLETING":
			mjobs.completing += 1

		case "COMPLETED":
			mjobs.completed += 1

		case "CONFIGURING":
			mjobs.configuring += 1

		case "FAILED":
			mjobs.failed += 1

		case "TIMEOUT":
			mjobs.timeout += 1

		case "PREEMPTED":
			mjobs.preempted += 1

		case "NODE_FAIL":
			mjobs.node_fail += 1
		}

		/* Reassign the metrics for the user */
		queue[partition].accounts[account].users[user].cpus = mcpus
		queue[partition].accounts[account].users[user].memory = mmemory
		queue[partition].accounts[account].users[user].gpus = mgpus
		queue[partition].accounts[account].users[user].jobs = mjobs
        }

        return queue
}

type QueueCollector struct {
	jobs *prometheus.Desc
	cpus *prometheus.Desc
	mem *prometheus.Desc
	gpus *prometheus.Desc
}

func NewQueueCollector() *QueueCollector {
	labelsQueue:= []string{"partition", "account", "state", "user"}

        return &QueueCollector{
		jobs: prometheus.NewDesc("slurm_queue_jobs",
			"Amount of Jobs in the Queue", labelsQueue, nil),

		cpus: prometheus.NewDesc("slurm_queue_cpus",
			"Amount of CPUs in the Queue", labelsQueue, nil),

		mem: prometheus.NewDesc("slurm_queue_mem",
			"Amount of Memory in the Queue", labelsQueue, nil),

		gpus: prometheus.NewDesc("slurm_queue_gpus",
			"Amount of GPUs in the Queue", labelsQueue, nil),

        }
}

func (qc *QueueCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- qc.jobs
	ch <- qc.mem
        ch <- qc.cpus
        ch <- qc.gpus
}

func (qc *QueueCollector) Collect(ch chan<- prometheus.Metric) {
        qm := QueueGetMetrics()

        for p := range qm {
		for acc := range qm[p].accounts {
			for user := range qm[p].accounts[acc].users {

				if qm[p].accounts[acc].users[user].jobs.pending > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.pending, p, acc, "pending", user)
				}

				if qm[p].accounts[acc].users[user].jobs.pending_dep > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.pending_dep, p, acc, "pending_dep", user)
				}

				if qm[p].accounts[acc].users[user].jobs.running > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.running, p, acc, "running", user)
				}

				if qm[p].accounts[acc].users[user].jobs.suspended > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.suspended, p, acc, "suspended", user)
				}

				if qm[p].accounts[acc].users[user].jobs.cancelled > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.cancelled, p, acc, "cancelled", user)
				}

				if qm[p].accounts[acc].users[user].jobs.completing > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.completing, p, acc, "completing", user)
				}

				if qm[p].accounts[acc].users[user].jobs.completed > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.completed, p, acc, "completed", user)
				}

				if qm[p].accounts[acc].users[user].jobs.configuring > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.configuring, p, acc, "configuring", user)
				}

				if qm[p].accounts[acc].users[user].jobs.failed > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.failed, p, acc, "failed", user)
				}

				if qm[p].accounts[acc].users[user].jobs.timeout > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.timeout, p, acc, "timeout", user)
				}

				if qm[p].accounts[acc].users[user].jobs.preempted > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.preempted, p, acc, "preempted", user)
				}

				if qm[p].accounts[acc].users[user].jobs.node_fail > 0 {
					ch <- prometheus.MustNewConstMetric(qc.jobs, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].jobs.node_fail, p, acc, "node_fail", user)
				}

				if qm[p].accounts[acc].users[user].cpus.pending > 0 {
					ch <- prometheus.MustNewConstMetric(qc.cpus, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].cpus.pending, p, acc, "pending", user)
				}

				if qm[p].accounts[acc].users[user].cpus.allocated > 0 {
					ch <- prometheus.MustNewConstMetric(qc.cpus, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].cpus.allocated, p, acc, "allocated", user)
				}

				if qm[p].accounts[acc].users[user].memory.pending > 0 {
					ch <- prometheus.MustNewConstMetric(qc.mem, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].memory.pending, p, acc, "pending", user)
				}

				if qm[p].accounts[acc].users[user].memory.allocated > 0 {
					ch <- prometheus.MustNewConstMetric(qc.mem, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].memory.allocated, p, acc, "allocated", user)
				}

				if qm[p].accounts[acc].users[user].gpus.allocated > 0 {
					ch <- prometheus.MustNewConstMetric(qc.gpus, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].gpus.allocated, p, acc, "allocated", user)
				}

				if qm[p].accounts[acc].users[user].gpus.pending > 0 {
					ch <- prometheus.MustNewConstMetric(qc.gpus, prometheus.GaugeValue,
						qm[p].accounts[acc].users[user].gpus.pending, p, acc, "pending", user)
				}
			}
		}
        }
}
