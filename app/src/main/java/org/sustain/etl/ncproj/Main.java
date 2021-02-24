package org.sustain.etl.ncproj;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "ncproj", mixinStandardHelpOptions = true,
    description = "project geometries onto netcdf data",
    subcommands = { DumpCommand.class, IndexCommand.class })
public class Main implements Callable<Integer> {
    @Override
    public Integer call() throws Exception {
        CommandLine.usage(new Main(), System.out);
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}
