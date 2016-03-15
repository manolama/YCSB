package com.yahoo.ycsb.measurements;

import java.io.IOException;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

public class OneMeasurementSingle extends OneMeasurement {

  private int measurement;
  
  public OneMeasurementSingle(String _name) {
    super(_name);
  }

  @Override
  public void measure(int latency) {
    measurement = latency;
  }

  @Override
  public String getSummary() {
    return Integer.toString(measurement);
  }

  @Override
  public void exportMeasurements(MeasurementsExporter exporter)
      throws IOException {
    exporter.write(getName(),  "Measure", measurement);
  }

}
