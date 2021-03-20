/* Copyright 2021 Victor Penso

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

type FairShareMetrics struct {
	effectv_usage float64
	alloc_cpu_minutes float64
	alloc_mem_minutes float64
}

func FairShareGetMetrics() map[string]*FairShareMetrics {
	return ParseFairShareMetrics(FairShareData())
}

func FairShareData() []byte {
        cmd := exec.Command( "sshare", "-n", "-P", "-o", "Account,EffectvUsage,TRESRunMins" )
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

func ParseFairShareMetrics(input []byte) map[string]*FairShareMetrics {
        accounts := make(map[string]*FairShareMetrics)

        lines := strings.Split(string(input), "\n")
        for _, line := range lines {
                if ! strings.HasPrefix(line,"  ") {
                        if strings.Contains(line,"|") {

				/* Get account name */
                                account := strings.Trim(strings.Split(line,"|")[0]," ")

                                _,key := accounts[account]
                                if !key {
                                        accounts[account] = &FairShareMetrics{0, 0, 0}
                                }

				/* Get the effective Usage of the account
                                 * With the FAIR_TREE algorithm (default), accounts don't have a FairShare value directly,
                                 * they rather have a LevelFS that helps sorting for usage
                                 */
                                effectv_usage,_ := strconv.ParseFloat(strings.Split(line,"|")[1],64)

				/* Parse TRESRunMins (Only CPU and Memory minutes) */
				tres_run_mins := strings.Trim(strings.Split(line,"|")[2]," ")

				cpu_tres := strings.Trim(strings.Split(tres_run_mins,",")[0]," ")
				mem_tres := strings.Trim(strings.Split(tres_run_mins,",")[1]," ")

				cpu_minutes,_ := strconv.ParseFloat(strings.Split(cpu_tres, "=")[1], 64)
				mem_minutes,_ := strconv.ParseFloat(strings.Split(mem_tres, "=")[1], 64)

				accounts[account].alloc_cpu_minutes = cpu_minutes
				accounts[account].alloc_mem_minutes = mem_minutes
				accounts[account].effectv_usage = effectv_usage
                        }
                }
        }
        return accounts
}

type FairShareCollector struct {
        effectv_usage     *prometheus.Desc
        alloc_cpu_minutes *prometheus.Desc
        alloc_mem_minutes *prometheus.Desc
}

func NewFairShareCollector() *FairShareCollector {
        labels := []string{"account"}
        return &FairShareCollector{
                effectv_usage: prometheus.NewDesc("slurm_account_effectv_usage", "EffectiveUsage for Account " , labels, nil),
		alloc_cpu_minutes: prometheus.NewDesc("slurm_account_cpu_minutes", "Allocated CPU-Minutes for Account" , labels, nil),
		alloc_mem_minutes: prometheus.NewDesc("slurm_account_mem_minutes", "Allocated MEM-Minutes for Account" , labels, nil),
        }
}

func (fsc *FairShareCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- fsc.effectv_usage
	ch <- fsc.alloc_cpu_minutes
	ch <- fsc.alloc_mem_minutes
}

func (fsc *FairShareCollector) Collect(ch chan<- prometheus.Metric) {
        fsm := FairShareGetMetrics()
        for f := range fsm {
                ch <- prometheus.MustNewConstMetric(fsc.effectv_usage, prometheus.GaugeValue, fsm[f].effectv_usage, f)
                ch <- prometheus.MustNewConstMetric(fsc.alloc_cpu_minutes, prometheus.GaugeValue, fsm[f].alloc_cpu_minutes, f)
                ch <- prometheus.MustNewConstMetric(fsc.alloc_mem_minutes, prometheus.GaugeValue, fsm[f].alloc_mem_minutes, f)
        }
}
