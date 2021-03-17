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

type CPUMetrics struct {
	alloc float64
	idle float64
	other float64
	total float64
	load float64
}

type MemoryMetrics struct {
	alloc float64
	free float64
	total float64
}

//type NodeGPUMetrics struct {
//
//}/

type NodeMetrics struct {
	state string
//	partition string
	cpus CPUMetrics
	memory MemoryMetrics
}

type PartitionMetrics struct {
	cpus CPUMetrics
	memory MemoryMetrics
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

func GetMetrics() (map[string]*NodeMetrics, map[string]*PartitionMetrics) {
	return ParseMetrics(Data())
}

/*
 * Make seperate Metrics for Partition Info and unique Node info
*/

func NewNodeMetrics(cm CPUMetrics, mm MemoryMetrics, state string) *NodeMetrics {

	node := &NodeMetrics{
		state,
		cm,
		mm,
	}

	return node
}

func AddToPartitionMetrics(partition string, partitions map[string]*PartitionMetrics, cm CPUMetrics, mm MemoryMetrics) {

	partitions[partition].cpus.alloc += cm.alloc
	partitions[partition].cpus.idle += cm.idle
	partitions[partition].cpus.other += cm.other
	partitions[partition].cpus.total += cm.total
	partitions[partition].cpus.load += cm.load

	partitions[partition].memory.alloc += mm.alloc
	partitions[partition].memory.free += mm.free
	partitions[partition].memory.total += mm.total
}

func NewPartitionMetrics(cm CPUMetrics, mm MemoryMetrics) *PartitionMetrics {
	partition := &PartitionMetrics{
		cm,
		mm,
	}

	return partition
}

func ParseMetrics(input []byte) (map[string]*NodeMetrics, map[string]*PartitionMetrics) {
	nodes := make(map[string]*NodeMetrics)
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
			cm := CPUMetrics{
				cpus_alloc,
				cpus_idle,
				cpus_other,
				cpus_total,
				(cpus_load / cpus_total) * 100.0,
			}

			mm := MemoryMetrics{
				mem_alloc,
				mem_free,
				mem_total,
			}

			/* Name of Partition */
			partition := split[7]

			_,pkey := partitions[partition]

			if !pkey {
				/* Create new Partition specific Metric */
				partitions[partition] = NewPartitionMetrics(cm, mm)
			} else {
				/* Otherwise add new info to existing record */
				AddToPartitionMetrics(partition, partitions, cm, mm)
			}

			/* Name of the node */
			node := split[0]
			/* State of the node */
			state := split[1]

			/* From this point on, we are only interested in unique Nodes,
                         * but sinfo will print each node once per partition, so ignore
                         * already seen nodes
                         */
			if _, seen := visited[node]; !seen {
				visited[node] = true
			} else {
				continue
			}

			_,nkey := nodes[node]

			if !nkey {
				/* Create new Node specific Metric */
				nodes[node] = NewNodeMetrics(cm, mm, state)
			}
		}
	}

	return nodes,partitions
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

type NPCollector struct {
	pc PartitionsCollector
	nc NodesCollector
}

type NodesCollector struct {
	states *prometheus.Desc
	cpus *prometheus.Desc
	mem *prometheus.Desc
	load *prometheus.Desc
}

type PartitionsCollector struct {
	cpus *prometheus.Desc
	mem *prometheus.Desc
	load *prometheus.Desc
}

func NewNPCollector() *NPCollector {
	plabelsCPUs := []string{"partition", "state"}
	plabelsMem := []string{"partition", "state"}
	plabelsLoad := []string{"partition"}

	nlabelsCPUs := []string{"host", "state"}
	nlabelsMem := []string{"host", "state"}
	nlabelsLoad := []string{"host"}
	nlabelsState := []string{"state"}

	return &NPCollector{
		PartitionsCollector{
			load: prometheus.NewDesc("slurm_partition_load",
				"Total CPU-Load in a Partition", plabelsLoad, nil),
			cpus: prometheus.NewDesc("slurm_partition_cpus",
				"CPUs in a Partition", plabelsCPUs, nil),
			mem: prometheus.NewDesc("slurm_partition_mem",
				"Memory in a Partition", plabelsMem, nil),
		},
		NodesCollector{
			states: prometheus.NewDesc("slurm_node_states",
				"States of a Node", nlabelsState, nil),
			load: prometheus.NewDesc("slurm_node_load",
				"Load of a Node", nlabelsLoad, nil),
			cpus: prometheus.NewDesc("slurm_node_cpus",
				"CPUs on a Node", nlabelsCPUs, nil),
			mem: prometheus.NewDesc("slurm_node_mem",
				"Memory on a Node", nlabelsMem, nil),
		},
	}
}

func (np *NPCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- np.nc.states
	ch <- np.nc.cpus
	ch <- np.nc.mem
	ch <- np.nc.load

	ch <- np.pc.cpus
	ch <- np.pc.mem
	ch <- np.pc.load
}

func (np *NPCollector) Collect(ch chan<- prometheus.Metric) {
	nodes,partitions := GetMetrics()
	states := make(map[string]float64)

	nc := &np.nc
	pc := &np.pc

	for node := range nodes {

		/* Count the states */
		states[nodes[node].state] += 1

		/* Create Node metrics */
		if nodes[node].cpus.alloc > 0 {
			ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, nodes[node].cpus.alloc, node, "allocated")
		}

		if nodes[node].cpus.idle > 0 {
			ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, nodes[node].cpus.idle, node, "idle")
		}

		if nodes[node].cpus.other > 0 {
			ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, nodes[node].cpus.other, node, "other")
		}

		if nodes[node].cpus.total > 0 {
			ch <- prometheus.MustNewConstMetric(nc.cpus, prometheus.GaugeValue, nodes[node].cpus.total, node, "total")
		}

		if nodes[node].memory.alloc > 0 {
			ch <- prometheus.MustNewConstMetric(nc.mem, prometheus.GaugeValue, nodes[node].memory.alloc, node, "allocated")
		}

		if nodes[node].memory.free > 0 {
			ch <- prometheus.MustNewConstMetric(nc.mem, prometheus.GaugeValue, nodes[node].memory.free, node, "free")
		}

		if nodes[node].memory.total > 0 {
			ch <- prometheus.MustNewConstMetric(nc.mem, prometheus.GaugeValue, nodes[node].memory.total, node, "total")
		}

		if nodes[node].cpus.load > 0 {
			ch <- prometheus.MustNewConstMetric(nc.load, prometheus.GaugeValue, nodes[node].cpus.load, node)
		}
	}


	/* Each State receives it's own metric with respective count */

	for state := range states {
		if states[state] > 0 {
			ch <- prometheus.MustNewConstMetric(nc.states, prometheus.GaugeValue, states[state], state)
		}
	}

	for p := range partitions {

		/* Create Partition metrics */
		if partitions[p].cpus.alloc > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus, prometheus.GaugeValue, partitions[p].cpus.alloc, p, "allocated")
		}

		if partitions[p].cpus.idle > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus, prometheus.GaugeValue, partitions[p].cpus.idle, p, "idle")
		}

		if partitions[p].cpus.other > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus, prometheus.GaugeValue, partitions[p].cpus.other, p, "other")
		}

		if partitions[p].cpus.total > 0 {
			ch <- prometheus.MustNewConstMetric(pc.cpus, prometheus.GaugeValue, partitions[p].cpus.total, p, "total")
		}

		if partitions[p].memory.alloc > 0 {
			ch <- prometheus.MustNewConstMetric(pc.mem, prometheus.GaugeValue, partitions[p].memory.alloc, p, "allocated")
		}

		if partitions[p].memory.free > 0 {
			ch <- prometheus.MustNewConstMetric(pc.mem, prometheus.GaugeValue, partitions[p].memory.free, p, "free")
		}

		if partitions[p].memory.total > 0 {
			ch <- prometheus.MustNewConstMetric(pc.mem, prometheus.GaugeValue, partitions[p].memory.total, p, "total")
		}

		if partitions[p].cpus.load > 0 {
			ch <- prometheus.MustNewConstMetric(pc.load, prometheus.GaugeValue, partitions[p].cpus.load, p)
		}
	}

}
