#/usr/bin/perl
use strict;
use warnings;
use JSON;
use File::Basename;

my $bin = "bin/ycsb";
my $status_interval = "1";
my $hdrout = "false";

my $mode = 2;  # 0 = run ycsb, 1 = run repeating, 2 = gen csv

my $threads = 1;
my $repeats = 50;
my @ops = ("load", "run");
my @dbs = ("asynchbase", "hbase10");
my $dps = 100000;
my @db_options = ("-p columnfamily=cf", "-p columnfamily=cf");
my @workloads = ("workloada");

if ($mode == 0) {
  
} elsif ($mode == 1) {
  runWorkloadsRepeating();
} elsif ($mode == 2) {
  processIntoCSVs();
}

print("All done!!\n");

sub runWorkloadsRepeating {
  for (my $i = 0; $i < $repeats; $i++) {
    my $db_idx = 0;
    foreach my $db (@dbs) {
      foreach my $workload (@workloads) {
        foreach my $op (@ops) {
          my $file_prefix = "results/".$db."_iteration_".$i."_threads_".$threads."_dps_".$dps."_op_".$op;
          my $cmd = "$bin $op $db -P workloads/$workload -threads $threads -s -p recordcount=$dps ";
          $cmd .= "-p status.interval=$status_interval -p measurement.trackjvm=true ";
          $cmd .= "-p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=$hdrout"."_ ";
          $cmd .= "-p hdrhistogram.output.path=$file_prefix ";
          $cmd .= "-p exporter=com.yahoo.ycsb.measurements.exporter.JSONArrayMeasurementsExporter ";
          $cmd .= "-p exportfile=$file_prefix.json ";
          $cmd .= $db_options[$db_idx];
          
          print("Running command: $cmd\n");
          my $output = `$cmd 2>&1`;
          print($output."\n");
        }
      }
      $db_idx++;
    }
  }
}

# NOT DONE YET
sub runIncrementingWorkloads {
 my $db_idx = 0;
  foreach my $db (@dbs) {
    foreach my $workload (@workloads) {
      foreach my $op (@ops) {
        my $file_prefix = "results/".$db."_threads_".$threads."_dps_".$dps."_op_".$op;
        my $cmd = "$bin $op $db -P workloads/$workload -threads $threads -s -p recordcount=$dps ";
        $cmd .= "-p status.interval=$status_interval -p measurement.trackjvm=true ";
        $cmd .= "-p measurementtype=hdrhistogram -p hdrhistogram.fileoutput=$hdrout"."_ ";
        $cmd .= "-p hdrhistogram.output.path=$file_prefix ";
        $cmd .= "-p exporter=com.yahoo.ycsb.measurements.exporter.JSONArrayMeasurementsExporter ";
        $cmd .= "-p exportfile=$file_prefix.json ";
        $cmd .= $db_options[$db_idx];
        
        print("Running command: $cmd\n");
        my $output = `$cmd 2>&1`;
        print($output."\n");
      }
    }
    $db_idx++;
  }
}

# Run through the resulting JSON files merge them into CSVs for loading into excel
sub processIntoCSVs {
 my @files = <./results/*>;
  
  my $async_load_map;
  my @async_load_types;
  my $hbase_load_map;
  my @hbase_load_types;
  my $async_run_map;
  my @async_run_types;
  my $hbase_run_map;
  my @hbase_run_types;
  
  foreach my $file (@files) {
    if ($file =~ /\.csv$/) {
      next;
    }
    
    my $FILE;
    open $FILE, $file or die "Couldn't open file: $!";
    my $raw;
    while (<$FILE>){
      $raw .= $_;
    }
    close $FILE;
    
    my $json = from_json($raw);
    my $base = "".basename($file)."";  # WFT? Why I need to do this?
    my $is_load = 0;
    my $is_async = 0;
    if ($base =~ s/asynchbase_//) {
      $is_async = 1;
    } else {
       $base =~ s/hbase10_//;
    }
    
    if ($base =~ /load/) {
      $is_load = 1;
      if ($is_async) { 
        push(@async_load_types, $base); 
      } else {
        push(@hbase_load_types, $base); 
      }
    } else {
      if ($is_async) { 
        push(@async_run_types, $base); 
      } else { 
        push(@hbase_run_types, $base); 
      }
    }
        
    if (!($base =~ /^([a-zA-Z0-9_]+)\.json$/g)) {
      print ("No match?!?!?!  ".basename($file)."\n");
      next;
    }
    my $line = $1.",";
    foreach my $obj (@{$json}) {
      if ($is_load) {
        if ($is_async) {
          buildMap(\%{$async_load_map}, $obj);
        } else {
          buildMap(\%{$hbase_load_map}, $obj);
        }
      } else {
        if ($is_async) {
          buildMap(\%{$async_run_map}, $obj);
        } else {
          buildMap(\%{$hbase_run_map}, $obj);
        }
      }
    }
    
    print("Processed $file\n");
  }
  
  writeCSV('results/async_load.csv', $async_load_map, @async_load_types);
  writeCSV('results/async_run.csv', $async_run_map, @async_run_types);
  writeCSV('results/hbase_load.csv', $hbase_load_map, @hbase_load_types);
  writeCSV('results/hbase_run.csv', $hbase_run_map, @hbase_run_types);
}

# Build a map of results ordered
sub buildMap {
  my ($map, $obj) = @_;
  
  if ($map->{$obj->{metric}.$obj->{measurement}}) {
    push(@{$map->{$obj->{metric}.$obj->{measurement}}}, $obj->{value});
    #print "Added entry: ".$obj->{metric}.$obj->{measurement}. "\n";
  } else {
    my @array;
    push (@array, $obj->{value});
    $map->{$obj->{metric}.$obj->{measurement}} = \@array;
    #print "new entry: ".$obj->{metric}.$obj->{measurement}."\n";
  }
}

# Dump the given map to the given file
sub writeCSV {
  my ($file, $map, @types) = @_;
  
  open(my $csv, '>', $file);
  # header
  print($csv "type");
  my @keys = keys %{$map};
  @keys = sort(@keys);
  foreach my $key(@keys) {
    print($csv ",$key");
  }
  print($csv "\n");
  
  # data
  for (my $i = 0; $i < @types; $i++) {
    print($csv $types[$i]);
    for my $key(@keys) {
      print($csv ",".$map->{$key}[$i]);
    }
    print($csv "\n");
  }
  close ($csv);
  print("Generated $file\n");
}