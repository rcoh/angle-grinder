# angle-grinder
Slice and dice log files on the command line.

[![asciicast](https://asciinema.org/a/bEjKsArIFgOOnxzb1FMZMWPhh.png)](https://asciinema.org/a/bEjKsArIFgOOnxzb1FMZMWPhh)

Not the fanciest way to analyze log files, just like an angle-grinder, it will probably work. If you've ever been on a server, and wished you could have just a _little_ bit more power than tailing and grepping, this is for you.

## Query Synax


```
<filter> | operator1 | operator2 | operator3 | ...
```

### Filters

Filters may be `*` or `filter!"`. Only lines containing `filter!` will be passed to the subsequent operators.
![filter.gif](/screen_shots/filter.gif)

### Operators

- `json`: Extract json-serialized rows into fields for later use
![json.gif](/screen_shots/json.gif)
- `parse "* pattern * otherpattern *" as a,b,c`: Parse text that matches the pattern into variables. Lines that don't match this pattern will be dropped. `*` is equivalent to `.*` and is greedy.
![parse.gif](/screen_shots/parse.gif)
- `count [by a, b] [as count_column]`: Count (potentially by key columns). Defaults to `_count` unless overridden with an `as` clause.
![count.gif](/screen_shots/count.gif)
- `sum(column) [by a, b] [as sum_column]`: Sum values in `column`. If the value in `column` is non-numeric, the row will be ignored.
- `average(column) [by a, b] as [average_column]`: Average values in `column`. If the value in `column` is non-numeric, the row will be ignored.

### Rendering
Non-aggregate data is simply written row-by-row to the terminal as it is received:
```
tail -f live_pcap | ag '* | parse "* > *:" as src, dest | parse "length *" as length'                           
[dest=111.221.29.254.https]        [length=0]        [src=21:50:18.458331 IP 10.0.2.243.47152]
[dest=111.221.29.254.https]        [length=310]      [src=21:50:18.458527 IP 10.0.2.243.47152]
```

Aggregate data is written to the terminal and will live-update until EOF:
```
k2                  avg         
--------------------------------
test longer test    500.50      
test test           375.38      
alternate input     4.00        
hello               3.00        
hello thanks        2.00        
```

The renderer will do its best to keep the data nicely formatted as it changes and the number of output rows is limited to the length of your terminal.

### Contributing
See the open issues for potential improvements/issues.
