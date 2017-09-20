package com.sncf.fab.ppiv.Monitoring

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit._

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{Gauge, MetricFilter, MetricRegistry, SharedMetricRegistries}
import com.sncf.fab.ppiv.utils.AppConf


object GraphiteConf {

  lazy val config = AppConf

  val graphite = new Graphite(new InetSocketAddress(
    config.metricHost, config.metricPort))


  lazy val prefix: String = config.metricPrefix

 // val mR = new MetricRegistry().register(MetricRegistry.name("",""), new Gauge[String]{})

  val reporter = GraphiteReporter.
    forRegistry(SharedMetricRegistries.getOrCreate(prefix))
    .prefixedWith(s"$prefix.${java.net.InetAddress.getLocalHost.getHostName}")
    .convertRatesTo(SECONDS)
    .convertDurationsTo(MILLISECONDS)
    .filter(MetricFilter.ALL)
    .build(graphite)


    println("carac : " + reporter.toString)
    def startGraphite(): Unit = {
    if (config.metricEnabled) {
      println("GRAPHITE STARTED")
       println(graphite.isConnected())
      reporter.start(config.metricRefreshInterval, SECONDS)
      reporter.report()
      println(graphite.isConnected())
    }
  }
}