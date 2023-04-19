/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Avro
{
    public class AvroGenTool
    {
        private const string UNDEFINED_MESSAGE = "Undefined name: ";
        public static int Main(string[] args)
        {
            // Print usage if no arguments provided
            if (args.Length == 0)
            {
                Usage();
                return 1;
            }

            // Print usage if help requested
            if (args.Contains("-h") || args.Contains("--help"))
            {
                Usage();
                return 0;
            }

            if (args.Contains("--version") || args.Contains("-V"))
            {
                // Print version information
                // Note: Use InformationalVersion attribute
                // It is capable to include semver prerelease information label (if prerelease), e.g. 1.x.y-beta.z
                Console.WriteLine(typeof(AvroGenTool).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion);
                return 0;
            }

            // Parse command line arguments
            bool? isProtocol = null;
            string inputFile = null;
            string outputDir = null;
            bool skipDirectoriesCreation = false;
            var namespaceMapping = new Dictionary<string, string>();
            for (int i = 0; i < args.Length; ++i)
            {
                if (args[i] == "-p")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.Error.WriteLine("Missing path to protocol file");
                        Usage();
                        return 1;
                    }

                    isProtocol = true;
                    inputFile = args[++i];
                }
                else if (args[i] == "-s")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.Error.WriteLine("Missing path to schema file");
                        Usage();
                        return 1;
                    }

                    isProtocol = false;
                    inputFile = args[++i];
                }
                else if (args[i] == "--namespace")
                {
                    if (i + 1 >= args.Length)
                    {
                        Console.Error.WriteLine("Missing namespace mapping");
                        Usage();
                        return 1;
                    }

                    var parts = args[++i].Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length != 2)
                    {
                        Console.Error.WriteLine("Malformed namespace mapping. Required format is \"avro.namespace:csharp.namespace\"");
                        Usage();
                        return 1;
                    }

                    namespaceMapping[parts[0]] = parts[1];
                }
                else if (args[i] == "--skip-directories")
                {
                    skipDirectoriesCreation = true;
                }
                else if (outputDir == null)
                {
                    outputDir = args[i];
                }
                else
                {
                    Console.Error.WriteLine("Unexpected command line argument: {0}", args[i]);
                    Usage();
                }
            }

            // Ensure we got all the command line arguments we need
            bool isValid = true;
            int rc = 0;
            if (!isProtocol.HasValue || inputFile == null)
            {
                Console.Error.WriteLine("Must provide either '-p <protocolfile>' or '-s <schemafile>'");
                isValid = false;
            }
            else if (outputDir == null)
            {
                Console.Error.WriteLine("Must provide 'outputdir'");
                isValid = false;
            }


            if (!isValid)
            {
                Usage();
                rc = 1;
            }
            else if (isProtocol.Value)
                rc = GenProtocol(inputFile, outputDir, namespaceMapping);
            else
                rc = GenSchema(inputFile, outputDir, namespaceMapping, skipDirectoriesCreation);

            return rc;
        }

        static void Usage()
        {
            Console.WriteLine("{0}\n\n" +
                "Usage:\n" +
                "  avrogen -p <protocolfile> <outputdir> [--namespace <my.avro.ns:my.csharp.ns>]\n" +
                "  avrogen -s <schemafile> <outputdir> [--namespace <my.avro.ns:my.csharp.ns>]\n\n" +
                "Options:\n" +
                "  -h --help        Show this screen.\n" +
                "  -V --version     Show version.\n" +
                "  --namespace      Map an Avro schema/protocol namespace to a C# namespace.\n" +
                "                   The format is \"my.avro.namespace:my.csharp.namespace\".\n" +
                "                   May be specified multiple times to map multiple namespaces.\n"  +
                "  --skip-directories Skip creation of namespace directories. It will generate classes right inside output directory\n",
                AppDomain.CurrentDomain.FriendlyName);
        }

        public static int GenProtocol(string infile, string outdir,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping)
        {
            try
            {
                string text = System.IO.File.ReadAllText(infile);

                CodeGen codegen = new CodeGen();
                codegen.AddProtocol(text, namespaceMapping);

                codegen.GenerateCode();
                codegen.WriteTypes(outdir);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception occurred. " + ex.Message);
                return 1;
            }

            return 0;
        }

        public static int GenSchema(string rootPath, string outdir,
            IEnumerable<KeyValuePair<string, string>> namespaceMapping, bool skipDirectories)
        {
            CodeGen codegen = new CodeGen();
            
            try
            {
                // Get all files from the path
                var files = System.IO.Directory.GetFiles(rootPath, "*.avsc", System.IO.SearchOption.AllDirectories);
                var names = new SchemaNames();
                var preParsedSchemas = new HashSet<String>();
                var writtenTypes = new HashSet<String>();

                // Generate code for each file
                foreach (var infile in files)
                {
                    generate(namespaceMapping, names, writtenTypes, preParsedSchemas, codegen, infile, outdir, skipDirectories);
                }

                codegen.GenerateCode();
                codegen.WriteTypes(writtenTypes, outdir, skipDirectories);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Exception occurred. " + ex.Message);
                return 1;
            }

            return 0;
        }

        private static void generate(IEnumerable<KeyValuePair<string, string>> namespaceMapping,
            SchemaNames names, ISet<String> writtenTypes, ISet<String> preParsedSchemas, CodeGen codegen, string infile, string outdir, bool skipDirectories, bool firstTry=true)
        {
            // Check if the file has already been parsed
            if (preParsedSchemas.Contains(infile))
                return;

            try
            {
                if(firstTry)
                    System.Console.WriteLine("Generating code for {0}", infile);
                var fileName = System.IO.Path.GetFileName(infile);
                var text = System.IO.File.ReadAllText(infile);

                codegen.AddSchema(text, names, namespaceMapping);

                // Add the file to the list of parsed files
                preParsedSchemas.Add(infile);
            }
            catch (SchemaParseException spe)
            {
                // Check for undefined name error and try to parse that file
                if (spe.Message.Contains(UNDEFINED_MESSAGE))
                {
                    var infilePath = System.IO.Path.GetDirectoryName(infile);
                    var undefinedName = spe.Message.Substring(spe.Message.IndexOf(UNDEFINED_MESSAGE) + UNDEFINED_MESSAGE.Length).Split(' ')[0];
                    var undefinedFile = System.IO.Path.Combine(infilePath, undefinedName + ".avsc");
                    if (System.IO.File.Exists(undefinedFile))
                    {
                        // Generate the missing schema
                        System.Console.WriteLine("Generating missing schema for {0}", undefinedFile);
                        generate(namespaceMapping, names, writtenTypes, preParsedSchemas, codegen, undefinedFile, outdir, skipDirectories);

                        // Add the file to the list of parsed files
                        preParsedSchemas.Add(undefinedFile);

                        // Retry the original schema
                        generate(namespaceMapping, names, writtenTypes, preParsedSchemas, codegen, infile, outdir, skipDirectories, false);
                    }
                    else
                    {
                        throw spe;
                    }
                }
                else
                {
                    throw spe;
                }
            }
        }
    }
}
