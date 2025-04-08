// Copyright 2025 Michael Bungenstock
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package udfexample;

import static org.apache.spark.sql.functions.udf;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import com.privacylogistics.FF3Cipher;

public class App {

  // must be static to avoid serialization issues in udf
  private static FF3Cipher cipher = new FF3Cipher("2DE79D232DF5585D68CE47882AE256D6", "CBD09280979564");
  private static UserDefinedFunction encrypt = udf(
      (String str) -> {
        if (str.length() < 6) {
          str = str + new String(new char[6 - str.length()]).replace('\0', ' ');
        }
        try {
          return cipher.encrypt(str);
        } catch (BadPaddingException e) {
          return "BadPaddingException";
        } catch (IllegalBlockSizeException e) {
          return "IllegalBlockSizeException";
        }
      }, DataTypes.StringType);

  private static String srcStr = "myproject.mydataset.customer";
  private static String dstStr = "myproject.mydataset.customer_new";
  private static String sqlStr = "SELECT CONCAT(encrypt(first_name),' - ',  last_name) AS name FROM source";

  private static void parseArguments(String[] args) {
    Options options = new Options();
    Option src = Option.builder("s").longOpt("src")
        .argName("src")
        .hasArg()
        .desc("BigQuery source table").build();
    options.addOption(src);
    Option dst = Option.builder("d").longOpt("dst")
        .argName("dst")
        .hasArg()
        .desc("BigQuery destination table").build();
    options.addOption(dst);
    Option query = Option.builder("q").longOpt("query")
        .argName("query")
        .hasArg()
        .desc("SQL query").build();
    options.addOption(query);

    CommandLine cmd;
    CommandLineParser parser = new DefaultParser();
    HelpFormatter helper = new HelpFormatter();
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("s")) {
        srcStr = cmd.getOptionValue("src");
      }
      if (cmd.hasOption("d")) {
        dstStr = cmd.getOptionValue("dst");
      }
      if (cmd.hasOption("q")) {
        sqlStr = cmd.getOptionValue("query");
      }
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      helper.printHelp("Usage:", options);
      System.exit(-1);
    }
  }

  public static void main(String[] args) {
    parseArguments(args);

    SparkSession spark = SparkSession.builder().appName("spark-bigquery-demo").getOrCreate();
    spark.udf().register("encrypt", encrypt);

    System.out.println("Src: " + srcStr);
    System.out.println("Dst: " + dstStr);
    System.out.println("Query: " + sqlStr);

    Dataset<Row> source = spark
        .read()
        .format("bigquery")
        .option("table", srcStr)
        .load()
        .cache();
    source.createOrReplaceTempView("source");

    Dataset<Row> result = spark.sql(sqlStr);

    result
        .write()
        .format("bigquery")
        .mode("append")
        .option("createDisposition", "CREATE_IF_NEEDED")
        .option("writeMethod", "direct") // use BQ Storage API!
        .option("table", dstStr)
        .save();
  }
}
