# angle-grinder [![Build Status](https://travis-ci.org/rcoh/angle-grinder.svg?branch=master)](https://travis-ci.org/rcoh/angle-grinder)
Slice and dice log files on the command line. 

Angle-grinder allows you to parse, aggregate, sum, average, percentile, and sort your data. You can see it, live-updating, in your terminal. Angle grinder is designed for when, for whatever reason, you don't have your data in graphite/honeycomb/kibana/sumologic/splunk/etc. but still want to be able to do sophisticated analytics.

Angle grinder can process about a million rows per second, so it's usable for fairly meaty aggregation. The results will live update in your terminal as data is processed. Angle grinder is a bare bones functional programming language coupled with a pretty terminal UI.

![overview gif](/screen_shots/overview.gif)

## Installation
Binaries are available for Linux and OS X: https://github.com/rcoh/angle-grinder/releases. Many more platforms (including Windows) are available if you compile from source.

Linux (statically linked with musl):
```
curl -L https://github.com/rcoh/angle-grinder/releases/download/v0.6.2/angle_grinder-v0.6.2-x86_64-unknown-linux-musl.tar.gz | tar Ozxf -  | sudo tee -a /usr/local/bin/agrind > /dev/null && sudo chmod +x /usr/local/bin/agrind
```

OS X:
```
curl -L https://github.com/rcoh/angle-grinder/releases/download/v0.6.2/angle_grinder-v0.6.2-x86_64-apple-darwin.tar.gz | tar Ozxf -  | sudo tee -a /usr/local/bin/agrind > /dev/null && sudo chmod +x /usr/local/bin/agrind
```

Or with Cargo:
```
cargo install ag
```

## Query Synax

```
agrind '<filter> | operator1 | operator2 | operator3 | ...'
```

### Filters

Filters may be `*` or `"filter!"` (must be enclosed in double quotes). Only lines containing `filter!` will be passed to the subsequent operators. `*` matches all lines.
![filter.gif](/screen_shots/filter.gif)

### Operators

- `json`: Extract json-serialized rows into fields for later use
![json.gif](/screen_shots/json.gif)
- `parse "* pattern * otherpattern *" [from field] as a,b,c`: Parse text that matches the pattern into variables. Lines that don't match this pattern will be dropped. `*` is equivalent to `.*` and is greedy. By default, `parse` operates on the raw text of the message. With `from field_name`, parse will instead process input from a specific column.
![parse.gif](/screen_shots/parse.gif)
- `fields [only|except|-|+] a, b`: Drop fields `a, b` or include only `a, b` depending on specified mode. Eg: `fields + event, timestamp`, `fields except event`. `-` is short for `except` and `+` is short for `only`.
- `count [by a, b] [as count_column]`: Count (potentially by key columns). Defaults to `_count` unless overridden with an `as` clause. eg: `* | count by source_host`
- `sum(column) [by a, b] [as sum_column]`: Sum values in `column`. If the value in `column` is non-numeric, the row will be ignored.
- `average(column) [by a, b] as [average_column]`: Average values in `column`. If the value in `column` is non-numeric, the row will be ignored.
- `pXX(column) [by a, b] [as pct_column]` eg. `p50(col)`, `p05(col)`, `p99(col)` calculate the XXth percentile of `column`.
- `sort by a, [b, c] [asc|desc]`: Sort aggregate data by a collection of columns. Defaults to ascending. 

### Example Queries
- Count the number of downloads of angle-grinder by release (with special guest jq)
``` 
curl  https://api.github.com/repos/rcoh/angle-grinder/releases  | \
   jq '.[] | .assets | .[]' -c | \
   ag '* | json 
         | parse "download/*/" from browser_download_url as version 
         | sum(download_count) by version | sort by version desc'
```
Output:
```
version       _sum
-----------------------
v0.6.2        0
v0.6.1        4
v0.6.0        5
v0.5.1        0
v0.5.0        4
v0.4.0        0
v0.3.3        0
v0.3.2        2
v0.3.1        9
v0.3.0        7
v0.2.1        0
v0.2.0        1
```
- Take the 50th percentile of response time by host:
```
tail -F my_json_logs | agrind '* | json | pct50(response_time) by url'
```
- Count the number of status codes by url:
```
tail -F  my_json_logs | agrind '* | json | count status_code by url'
```

### Rendering
Non-aggregate data is simply written row-by-row to the terminal as it is received:
```
tail -f live_pcap | agrind '* | parse "* > *:" as src, dest | parse "length *" as length' 
[dest=111.221.29.254.https]        [length=0]        [src=21:50:18.458331 IP 10.0.2.243.47152]
[dest=111.221.29.254.https]        [length=310]      [src=21:50:18.458527 IP 10.0.2.243.47152]
```

Aggregate data is written to the terminal and will live-update until the stream ends:
```
k2                  avg         
--------------------------------
test longer test    500.50      
test test           375.38      
alternate input     4.00        
hello               3.00        
hello thanks        2.00        
```

The renderer will do its best to keep the data nicely formatted as it changes and the number of output rows is limited to the length of your terminal. Currently,
it has a refresh rate of about 20hz.

### Contributing
`angle-grinder` builds with stable rust:
```
cargo build
cargo test
cargo install
agrind --help
```

See the open issues for potential improvements/issues.

### Related Work 
Angle Grinder is a rewrite of [Sumoshell](https://github.com/SumoLogic/sumoshell) written to be easier to use, testable and a better platform for new features.
