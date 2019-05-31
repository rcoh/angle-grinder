# angle-grinder [![Build Status](https://travis-ci.org/rcoh/angle-grinder.svg?branch=master)](https://travis-ci.org/rcoh/angle-grinder) [![Gitter chat](https://badges.gitter.im/gitterHQ/gitter.png)](https://gitter.im/angle-grinder/Lobby)
Slice and dice log files on the command line. 

Angle-grinder allows you to parse, aggregate, sum, average, percentile, and sort your data. You can see it, live-updating, in your terminal. Angle grinder is designed for when, for whatever reason, you don't have your data in graphite/honeycomb/kibana/sumologic/splunk/etc. but still want to be able to do sophisticated analytics.

Angle grinder can process well above 1M rows per second (simple pipelines as high as 5M), so it's usable for fairly meaty aggregation. The results will live update in your terminal as data is processed. Angle grinder is a bare bones functional programming language coupled with a pretty terminal UI.

![overview gif](/screen_shots/overview.gif)

## Installation
Binaries are available for Linux and OS X. Many more platforms (including Windows) are available if you compile from source. In all of the commands below, the resulting binary will be called `agrind`. Starting with `v0.9.0`, `agrind` can self-update via the `--self-update` flag.

### With Brew (OS X)
```bash
brew install angle-grinder
```

### With Curl (Single binary)
Linux:
```bash
curl -L https://github.com/rcoh/angle-grinder/releases/download/v0.9.0/angle_grinder-v0.9.0-x86_64-unknown-linux-musl.tar.gz \
  | tar Ozxf - \
  | sudo tee /usr/local/bin/agrind > /dev/null && sudo chmod +x /usr/local/bin/agrind
```

OS X:
```bash
curl -L https://github.com/rcoh/angle-grinder/releases/download/v0.9.0/angle_grinder-v0.9.0-x86_64-apple-darwin.tar.gz \
  | tar Ozxf - \
  | sudo tee /usr/local/bin/agrind > /dev/null && sudo chmod +x /usr/local/bin/agrind
```

### From Source

If you have Cargo installed, you can compile & install from source: (Works with Stable Rust >=1.26)
```bash
cargo install ag
```

## Query Syntax

An angle grinder query is composed of filters followed by a series of operators.
The filters select the lines from the input stream to be transformed by the operators.
Typically, the initial operators will transform the data in some way by parsing fields or JSON from the log line. 
The subsequent operators can then aggregate or group the data via operators like `sum`, `average`, `percentile`, etc.
```bash
agrind '<filter1> [... <filterN>] | operator1 | operator2 | operator3 | ...'
```

A simple query that operates on JSON logs and counts the number of logs per level could be:
```bash
agrind '* | json | count by log_level'
```

### Filters

There are three basic filters:

- `*`: Match all logs
- `filter-me*` (with no quotes) is a case-insensitive match that can include wildcards
- "filter-me" (in quotes) is a case-sensitive match (no wildcards, `*` matches literal `*`
, `filter-me`, or `"filter me!"`. 

Filters can be combined with `AND`, `OR` and `NOT` 
```agrind
("ERROR" OR WARN*) AND NOT staging | count
```

Sub-expressions _must_ be grouped in parenthesis. Only lines that match all filters will be passed to the subsequent operators.
![filter.gif](/screen_shots/filter.gif)

### Operators

#### Non Aggregate Operators
These operators have a 1 to 1 correspondence between input data and output data. 1 row in, 0 or 1 rows out.

##### JSON
`json [from other_field]`: Extract json-serialized rows into fields for later use. If the row is _not_ valid JSON, then it is dropped. Optionally, `from other_field` can be 
specified. Embedded JSON isn't natively supported, but you can use the `json from` to reparse nested keys (see examples).

*Examples*:
```agrind
* | json
```
```agrind
* | parse "INFO *" as js | json from js
```

Given input like:
```
{"key": "blah", "nested_key": {"this": "that"}}
```
```agrind
* | json | json from nested_key | fields this
```
![json.gif](/screen_shots/json.gif)

##### Parse
`parse "* pattern * otherpattern *" [from field] as a,b,c [nodrop]`: Parse text that matches the pattern into variables. Lines that don't match the pattern will be dropped unless `nodrop` is specified. `*` is equivalent to regular expression `.*` and is greedy.
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
`where <bool-expr>`: Drop rows where the condition is not met.
The condition must be an expression that returns a boolean value.
The expression can be as simple as a field name or a comparison (i.e. ==, !=, <=, >=, <, >) 
between fields and literal values (i.e. numbers, strings).
The '!' operator can be used to negate the result of a sub-expression.
Note that `None == None`, so a row where both the left and right sides match a non-existent key will match.

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

##### Limit
`limit #`: Limit the number of rows to the given amount.  If the number is positive, only the 
first N rows are returned.  If the number is negative, the last N rows are returned.

*Examples*
```agrind
* | limit 10
```
```agrind
* | limit -10
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

##### Total
`total(a) [as renamed_total]`: Compute the running total of a given field. Total does not currently support grouping!

*Examples*:
```agrind
* | json | total(num_requests) as tot_requests
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
cargo install --path .
agrind --help
```

See the following projects and open issues for specific potential improvements/bugs.

#### Project: Improving Error Reporting

Usability can be greatly improved by accurate and helpful error messages for query-related issues.
If you have struggled to figure out why a query is not working correctly and had a hard time 
fixing the issue, that would be a good place to jump in and start making changes!

First, you need to determine where the problem is occurring.
If the parser is rejecting a query, the grammar may need some tweaking to be more accepting of 
some syntax.
For example, if the field names are not provided for the `parse` operator, the query can still 
be parsed to produce a syntax tree and the error can be raised in the next phase.
If the query passes the parsing phase, the problem may lie in the semantic analysis phase where the
values in parse tree are verified for correctness.
Continuing with the `parse` example, if the number of captures in the pattern string does not 
match the number of field names, the error would be raised here.
Finally, if the query has been valid up to this point, you might want to raise an error at 
execution time.
For example, if a field name being accessed does not exist in the records being passed to an 
operator, an error could be raised to tell the user that they might have mistyped the name.

Once you have an idea of where the problem might lie, you can start to dig into the code.
The grammar is written using [nom](https://github.com/Geal/nom/) and is contained in the
[`lang.rs`](https://github.com/rcoh/angle-grinder/blob/master/src/lang.rs) module.
The enums/structs that make up the parse tree are also in the `lang.rs` module.
To make error reporting easier, values in the parse tree are wrapped with a `Positioned` object 
that records where the value came from in the query string.
The `Positioned` objects are produced by the `with_pos!()` parser combinator.
These objects can then be passed to the `SnippetBuilder` in the
[`errors.rs`](https://github.com/rcoh/angle-grinder/blob/master/src/errors.rs) module to highlight 
portions of the query string in error messages.

The semantic phase is contained in the
[`typecheck.rs`](https://github.com/rcoh/angle-grinder/blob/master/src/typecheck.rs) module and 
is probably where most of the work will need to be done.
The `semantic_analysis()` methods in that module are passed an `ErrorBuilder` that can be used to
build and send error reports to the user. 

After adjusting the grammar and adding a check for the problem, it will be time to figure out how
to inform the user.
Ideally, any errors should explain the problem, point the user to the relevant part of the query 
string, and lead the user to a solution.
Using the `ErrorBuilder`, you can call the `new_error_report_for()` method to construct a 
`SnippetBuilder` for a given error.
To highlight a portion of the query string, use the `with_code_pointer()` method with the
`Positioned` object that refers to the relevant segment of the query string.
Finally, additional help/examples can be added by calling the `with_resolution()` method.

Once you're all done, you should see a nicely formatted error message like the following:

```
error: Expecting an expression to count
  |
1 | * | count_distinct
  |     ^^^^^^^^^^^^^^ No field argument given
  |
  = help: example: count_distinct(field_to_count)
```

### Similar Projects
* Angle Grinder is a rewrite of [Sumoshell](https://github.com/SumoLogic/sumoshell) written to be easier to use, testable and a better platform for new features.
* [lnav](http://lnav.org/) is a full featured log analysis platform in your terminal (with many more features than angle-grinder). It includes support for common log file formats out-of-the-box, generalized SQL queries on your logs, auto-coloring and a whole host of other features.
* [visidata](http://visidata.org/) is a spreadsheets app in your terminal
