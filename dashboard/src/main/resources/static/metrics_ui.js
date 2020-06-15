/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const UPDATE_INTERVAL_MILLS = 1000;
const CHARTS_CONFIG = [
  {
    name: 'visits',
    endpoint: '/metrics/timeseries/visits',
    chartClass: 'google.visualization.BarChart',
    elementId: 'visits',
    metricName: 'Visits',
    title: 'Visits/min',
    chartOptions: {bars: 'vertical'},
    chartDataTransform: transformTimeSeriesToGraphData
  },
  {
    name: 'users',
    endpoint: '/metrics/timeseries/users',
    chartClass: 'google.visualization.BarChart',
    elementId: 'users',
    metricName: 'Unique Users',
    title: 'Unique Users/min',
    chartOptions: {bars: 'vertical'},
    chartDataTransform: transformTimeSeriesToGraphData
  },
  {
    name: 'experiements',
    endpoint: '/metrics/timeseries/experiments',
    chartClass: 'google.visualization.LineChart',
    elementId: 'experiments',
    metricName: 'Live Experiments',
    title: 'Live Experiments/min',
    chartDataTransform: transformTimeSeriesToGraphData
  },
  {
    name: 'variant_overlap',
    endpoint: '/metrics/timeseries/variantsOverlap',
    chartClass: 'google.visualization.Table',
    elementId: 'variant-overlap',
    metricName: 'Variant',
    title: 'Variant User Overlap',
    chartOptions: {width: '100%', height: '100%'},
    chartDataTransform: transformOverlapDataToGraphData
  }
];

const BASIC_CHART_OPTIONS = (chartConfig) => {
  return {title: chartConfig.title, animation: {duration: 500, easing: 'out'}}
};

let chartObjects = {};

let intervalTimerId = null;

function transformTimeSeriesToGraphData(chartMetricName) {
  return (timeSeriesData) => {
    const chartData = new google.visualization.DataTable();
    chartData.addColumn('date', 'Time of Day');
    chartData.addColumn('number', chartMetricName);
    chartData.addRows(timeSeriesData.map(
        item => [new Date(item['timestamp']), item.metric]));

    return chartData;
  };
}

function transformOverlapDataToGraphData(chartMetricName) {
  return (data) => {
    const uniqueDimensions = [...new Set(data.flatMap(x => x.dimensions))];
    const chartData = new google.visualization.DataTable();
    chartData.addColumn('string', chartMetricName);
    uniqueDimensions.forEach(
        dimension => chartData.addColumn('number', dimension));

    let overlapMap = {};
    data.forEach(item => {
      if (!overlapMap[item.dimensions[0]]) {
        overlapMap[item.dimensions[0]] = {};
      }
      overlapMap[item.dimensions[0]][item.dimensions[1]] = item.metric;
    });

    chartData.addRows(
        Object.keys(overlapMap).map(
            dimension => [dimension].concat(uniqueDimensions.map(
                overlapDimension => overlapMap[dimension][overlapDimension])))
    );

    return chartData;
  };
}

function drawChart(metricsJson, chartConfig) {
  const chartOptions = {
    ...BASIC_CHART_OPTIONS(chartConfig), ...((chartConfig.chartOptions)
        ? chartConfig.chartOptions : {})
  };

  const transformFunction = chartConfig.chartDataTransform(
      chartConfig.metricName);
  chartObjects[chartConfig.name]
  .draw(transformFunction(metricsJson), chartOptions);
}

function initAllCharts() {
  for (let chartConfig of CHARTS_CONFIG) {
    const classObj = eval(`${chartConfig.chartClass}`);
    chartObjects[chartConfig.name] = (new classObj(
        document.getElementById(chartConfig.elementId)));
    retrieveFromServerAndDraw(chartConfig)
  }
}

function retrieveAllConfigs() {
  CHARTS_CONFIG.forEach(config => retrieveFromServerAndDraw(config));
}

function retrieveFromServerAndDraw(chartConfig) {
  fetch(chartConfig.endpoint)
  .then(results => results.json())
  .then(results => drawChart(results, chartConfig));
}

function toggleAutoUpdate() {
  const updateBtn = document.getElementById('btn_auto_update');
  if (intervalTimerId) {
    clearInterval(intervalTimerId);
    intervalTimerId = null;
    updateBtn.style.background = '';
    updateBtn.style.color = 'black';
  } else {
    intervalTimerId = setInterval(retrieveAllConfigs, UPDATE_INTERVAL_MILLS);
    updateBtn.style.background = 'green';
    updateBtn.style.color = 'white';
  }
}

google.charts.load('current',
    {'packages': ['corechart', 'line', 'bar', 'table']});
google.charts.setOnLoadCallback(initAllCharts);