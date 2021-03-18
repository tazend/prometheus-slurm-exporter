/* Copyright 2017 Victor Penso, Matteo Dessalvi
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
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

type NodeCPUMetrics struct {
	alloc float64
	idle float64
	other float64
	total float64
	load float64
}

type NodeMemoryMetrics struct {
	alloc float64
	free float64
	total float64
}

//type NodeGPUMetrics struct {
//
//}/

type NodeMetrics struct {
	state string
	occurence int
	cpus NodeCPUMetrics
	memory NodeMemoryMetrics
}

type PartitionMetrics struct {
	nodes map[string]*NodeMetrics
}

func Data() []byte {
	cmd := exec.Command("sinfo", "-h", "-N",  "-ONodeList:.|,StateCompact:.,CPUsState:.,CPUsLoad:.,AllocMem:.,FreeMem:.,Memory:.,PartitionName:.")
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

func GetPartitionMetrics() map[string]*PartitionMetrics {
	return ParsePartitionMetrics(Data())
}

func NewNodeMetrics(cm NodeCPUMetrics, mm NodeMemoryMetrics, state string) *NodeMetrics {

	node := &NodeMetrics{
		state,
		1,
		cm,
		mm,
	}

	return node
}

func NewPartitionMetrics() *PartitionMetrics {

	partition := &PartitionMetrics{
		make(map[string]*NodeMetrics),
	}

	return partition
}

func ParsePartitionMetrics(input []byte) map[string]*PartitionMetrics {

	partitions := make(map[string]*PartitionMetrics)
	visited := map[string]bool{}

	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if strings.Contains(line, "|") {

			/* Split sinfo output */
			split := strings.Split(line, "|")

			cpu_states := split[2]
			cpus_alloc,_ := strconv.ParseFloat(strings.Split(cpu_states, "/")[0], 64)
			cpus_idle,_ := strconv.ParseFloat(strings.Split(cpu_states, "/")[1], 64)
			cpus_other,_ := strconv.ParseFloat(strings.Split(cpu_states, "/")[2], 64)
			cpus_total,_ := strconv.ParseFloat(strings.Split(cpu_states, "/")[3], 64)
			cpus_load,_ := strconv.ParseFloat(split[3], 64)

			mem_alloc,_ := strconv.ParseFloat(split[4], 64)
			mem_free,_ := strconv.ParseFloat(split[5], 64)
			mem_total,_ := strconv.ParseFloat(split[6], 64)

			/* Initialize Metrics */
			cm := NodeCPUMetrics{
				cpus_alloc,
				cpus_idle,
				cpus_other,
				cpus_total,
				(cpus_load / cpus_total) * 100.0,
			}

			mm := NodeMemoryMetrics{
				mem_alloc,
				mem_free,
				mem_total,
			}

			/* Name of Partition */
			partition := split[7]

			_,pkey := partitions[partition]

			if !pkey {
				/* Create new Partition specific Metric */
				partitions[partition] = NewPartitionMetrics()
			}

			/* Name of the node */
			node := split[0]
			/* State of the node */
			state := split[1]

			partitions[partition].nodes[node] = NewNodeMetrics(cm, mm, state)

			if _, seen := visited[node]; !seen {
				visited[node] = true
			} else {
				partitions[partition].nodes[node].occurence += 1
				continue
			}
		}
	}

	return partitions
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

type NodesCollector struct {
	states *prometheus.Desc
	cpus *prometheus.Desc
	mem *prometheus.Desc
	load *prometheus.Desc
}

func NewNodesCollector() *NodesCollector {

	nlabelsResource := []string{"host", "state", "partition", "occurence"}
	nlabelsLoad := []string{"host", "partition", "occurence"}
	nlabelsState := []string{"host", "state", "partition", "occurence"}

	return &NodesCollector{
		states: prometheus.NewDesc("slurm_node_states",
			"State of a single Node", nlabelsState, nil),
		load: prometheus.NewDesc("slurm_node_load",
			"Load of a single Node", nlabelsLoad, nil),
		cpus: prometheus.NewDesc("slurm_node_cpus",
			"CPUs on a single Node", nlabelsResource, nil),
		mem: prometheus.NewDesc("slurm_node_mem",
			"Memory on a single Node", nlabelsResource, nil),
	}
}

func (nc *NodesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- nc.states
	ch <- nc.cpus
	ch <- nc.mem
	ch <- nc.load
}

func (nc *NodesCollector) Collect(ch chan<- prometheus.Metric) {
	partitions := GetPartitionMetrics()

	/* Resources per Node (multiple entries per node possible due to a node configured in multiple partitions) */
	for p := range partitions {
		for node := range partitions[p].nodes {

			/* This helps summing up the resource values of all unique nodes in the cluster */
			occ := strconv.Itoa(partitions[p].nodes[node].occurence)

			/* Create Node metrics */
			if partitions[p].nodes[node].cpus.alloc > 0 {
				ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, partitions[p].nodes[node].cpus.alloc,
					node, "allocated", p, occ)
			}

			if partitions[p].nodes[node].cpus.idle > 0 {
				ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, partitions[p].nodes[node].cpus.idle,
					node, "idle", p, occ)
			}

			if partitions[p].nodes[node].cpus.other > 0 {
				ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, partitions[p].nodes[node].cpus.other,
					node, "other", p, occ)
			}

			if partitions[p].nodes[node].cpus.total > 0 {
				ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, partitions[p].nodes[node].cpus.total,
					node, "total", p, occ)
			}

			if partitions[p].nodes[node].memory.alloc > 0 {
				ch <- prometheus.MustNewConstMetric(nc.mem, prometheus.GaugeValue, partitions[p].nodes[node].memory.alloc,
					node, "allocated", p, occ)
			}

			if partitions[p].nodes[node].memory.free > 0 {
				ch <- prometheus.MustNewConstMetric(nc.mem, prometheus.GaugeValue, partitions[p].nodes[node].memory.free,
					node, "free", p, occ)
			}

			if partitions[p].nodes[node].memory.total > 0 {
				ch <- prometheus.MustNewConstMetric(nc.mem, prometheus.GaugeValue, partitions[p].nodes[node].memory.total,
					node, "total", p, occ)
			}

			if partitions[p].nodes[node].cpus.load > 0 {
				ch <- prometheus.MustNewConstMetric(nc.load, prometheus.GaugeValue, partitions[p].nodes[node].cpus.load,
					node, p, occ)
			}

			ch <- prometheus.MustNewConstMetric(nc.states, prometheus.GaugeValue, 1,
				node, partitions[p].nodes[node].state, p, occ)
		}
	}
}
