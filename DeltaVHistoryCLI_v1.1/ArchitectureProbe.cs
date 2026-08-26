using System;
using System.Reflection;

namespace DeltaVHistoryCLI
{
    class ArchitectureProbe
    {
        static int Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage: ArchitectureProbe.exe <assembly.exe>");
                return 2;
            }

            AssemblyName assembly = AssemblyName.GetAssemblyName(args[0]);
            if (assembly.ProcessorArchitecture != ProcessorArchitecture.X86)
            {
                Console.WriteLine(
                    "ARCHITECTURE FAILED: " + args[0] + " is " +
                    assembly.ProcessorArchitecture.ToString());
                return 1;
            }

            Console.WriteLine("X86 OK: " + args[0]);
            return 0;
        }
    }
}
