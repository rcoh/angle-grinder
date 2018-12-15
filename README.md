# angle-grinder [![Build Status](https://travis-ci.org/rcoh/angle-grinder.svg?branch=master)](https://travis-ci.org/rcoh/angle-grinder) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/angle-grinder/Lobby)
Slice and dice log files on the command line. 

Angle-grinder allows you to parse, aggregate, sum, average, percentile, and sort your data. You can see it, live-updating, in your terminal. Angle grinder is designed for when, for whatever reason, you don't have your data in graphite/honeycomb/kibana/sumologic/splunk/etc. but still want to be able to do sophisticated analytics.

Angle grinder can process about a million rows per second, so it's usable for fairly meaty aggregation. The results will live update in your terminal as data is processed. Angle grinder is a bare bones functional programming language coupled with a pretty terminal UI.

![overview gif](/screen_shots/overview.gif)

## Installation
Binaries are available for Linux and OS X. Many more platforms (including Windows) are available if you compile from source. In all of the commands below, the resulting binary will be called `agrind`.

### With Brew (OS X)
```bash
brew install angle-grinder
```

### With Curl (Single binary)
Linux:
```bash
curl -L https://github.com/rcoh/angle-grinder/releases/download/v0.7.5/angle_grinder-v0.7.5-x86_64-unknown-linux-musl.tar.gz \
  | tar Ozxf - \
  | sudo tee /usr/local/bin/agrind > /dev/null && sudo chmod +x /usr/local/bin/agrind
```

OS X:
```bash
curl -L https://github.com/rcoh/angle-grinder/releases/download/v0.7.5/angle_grinder-v0.7.5-x86_64-apple-darwin.tar.gz \
  | tar Ozxf - \
  | sudo tee /usr/local/bin/agrind > /dev/null && sudo chmod +x /usr/local/bin/agrind
```

### From Source

If you have Cargo installed, you can compile & install from source: (Works with Stable Rust >=1.26)
```bash
cargo install ag
```

## Query Synax

An angle grinder query is composed of a filter followed by a series of operators. Typically, the initial operators will transform the data in some way by parsing fields or JSON from the log line. 
The subsequent operators can then aggregate or group the data via operators like `sum`, `average`, `percentile`, etc.
```bash
agrind '<filter> | operator1 | operator2 | operator3 | ...'
```

A simple query that operates on JSON logs and counts the number of logs per level could be:
```bash
agrind '* | json | count by log_level'
```

### Filters

Filters may be `*` or `"filter!"` (must be enclosed in double quotes). Only lines containing `filter!` will be passed to the subsequent operators. `*` matches all lines.
![filter.gif](/screen_shots/filter.gif)

### Operators

#### Non Aggregate Operators
These operators have a 1 to 1 correspondence between input data and output data. 1 row in, 0 or 1 rows out.

##### JSON
`json [from other_field]`: Extract json-serialized rows into fields for later use. If the row is _not_ valid JSON, then it is dropped. Optionally, `from other_field` can be 
specified.

*Examples*:
```agrind
* | json
```
```agrind
* | parse "INFO *" as js | json from js
```
![json.gif](/screen_shots/json.gif)

##### Parse
`parse "* pattern * otherpattern *" [from field] as a,b,c`: Parse text that matches the pattern into variables. Lines that don't match the pattern will be dropped. `*` is equivalent to regular expression `.*` and is greedy. 
By default, `parse` operates on the raw text of the message. With `from field_name`, parse will instead process input from a specific column.

*Examples*:
```agrind
* | parse "[status_code=*]" as status_code
```
![parse.gif](/screen_shots/parse.gif)

##### Fields
`fields [only|except|-|+] a, b`: Drop fields `a, b` or include only `a, b` depending on specified mode. 

*Examples*:
Drop all fields except `event` and `timestamp`
```agrind
* | json | fields + event, timestamp
```
Drop only the `event` field
```agrind
* | fields except event
```

##### Where
`where a == b`: Drop rows where the condition is not met. Note that `None == None`, so a row where both the left and right sides match a non-existent key will match.

*Examples*
```agrind
* | json | where status_code >= 400
```
```agrind
* | json | where user_id_a == user_id_b
```
```agrind
* | json | where url != "/hostname"
```
#### Aggregate Operators
Aggregate operators group and combine your data by 0 or more key fields. The same query can include multiple aggregates.
The general syntax is:
```noformat
(operator [as renamed_column])+ [by key_col1, key_col2]
```

In the simplest form, key fields refer to columns, but they can also be generalized expressions (see examples)
*Examples*:
```agrind
* | count
```
```agrind
* | json | count by status_code
```
```agrind
* | json | count, p50(response_ms), p90(response_ms) by status_code
```
```agrind
* | json | count as num_requests, p50(response_ms), p90(response_ms) by status_code
```
```agrind
* | json | count, p50(response_ms), p90(response_ms), count by status_code >= 400, url
```

There are several aggregate operators available.

##### Count
`count [as count_column]`: Counts the numer of input rows. Output column Defaults to `_count` 

*Examples*:

Count number of rows by `source_host`:
```agrind
* | count by source_host
```
Count number of source_hosts:
```agrind
* | count by source_host | count
```

##### Sum
`sum(column) [as sum_column]`: Sum values in `column`. If the value in `column` is non-numeric, the row will be ignored.
*Examples*:
```agrind
* | json | sum(num_records) by action
```

##### Average
`average(column) [as average_column] [by a, b] `: Average values in `column`. If the value in `column` is non-numeric, the row will be ignored.

*Examples*:
```agrind
* | json | average(response_time)
```

##### Percentile
`pXX(column)`: calculate the XXth percentile of `column`
*Examples*:
```agrind
* | json | p50(response_time), p90(response_time) by endpoint_url, status_code
```

##### Sort
`sort by a, [b, c] [asc|desc]`: Sort aggregate data by a collection of columns. Defaults to ascending. 
*Examples*:
```agrind
* | json | count by endpoint_url, status_code | sort by endpoint_url desc
```

##### Count Distinct
`count_distinct(a)`: Count distinct values of column `a`. Warning: this is not fixed memory. Be careful about processing too many groups.
*Examples*:
```agrind
* | json | count_distinct(ip_address)
```

### Example Queries
- Count the number of downloads of angle-grinder by release (with special guest jq)
```bash
curl  https://api.github.com/repos/rcoh/angle-grinder/releases  | \
   jq '.[] | .assets | .[]' -c | \
   agrind '* | json 
         | parse "download/*/" from browser_download_url as version 
         | sum(download_count) by version | sort by version desc'
```
Output:
```noformat
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
```bash
tail -F my_json_logs | agrind '* | json | pct50(response_time) by url'
```
- Count the number of status codes by url:
```bash
tail -F  my_json_logs | agrind '* | json | count status_code by url'
```
More example queries can be found in the [tests folder](tests/structured_tests)

### Rendering
Non-aggregate data is simply written row-by-row to the terminal as it is received:
```noformat
tail -f live_pcap | agrind '* | parse "* > *:" as src, dest | parse "length *" as length' 
[dest=111.221.29.254.https]        [length=0]        [src=21:50:18.458331 IP 10.0.2.243.47152]
[dest=111.221.29.254.https]        [length=310]      [src=21:50:18.458527 IP 10.0.2.243.47152]
```

Aggregate data is written to the terminal and will live-update until the stream ends:
```noformat
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
`angle-grinder` builds with Rust >= 1.26. There are a number of ways you can contribute:
- Adding new special purpose operators
- Improve documentation of existing operators + providing more usage examples
- Provide more test cases of real queries on real world data
- Tell more people about angle grinder!
```bash
cargo build
cargo test
cargo install
agrind --help
```

See the open issues for specific potential improvements/bugs.

### Similar Projects
* Angle Grinder is a rewrite of [Sumoshell](https://github.com/SumoLogic/sumoshell) written to be easier to use, testable and a better platform for new features.
* [lnav](http://lnav.org/) is a full featured log analysis platform in your terminal (with many more features than angle-grinder). It includes support for common log file formats out-of-the-box, generalized SQL queries on your logs, auto-coloring and a whole host of other features.
* [visidata](http://visidata.org/) is a spreadsheets app in your terminal
