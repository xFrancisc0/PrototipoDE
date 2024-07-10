"""import glob
import unittest

import os
current_directory = os.getcwd()
print(current_directory)

# Construir el patrón de búsqueda para los archivos de prueba
test_files_pattern = os.path.join(current_directory, '**', '*Tests.py')
print("Patrón de búsqueda:", test_files_pattern)

# Buscar archivos que coincidan con el patrón
test_files = glob.glob(test_files_pattern, recursive=True)
print("Archivos de prueba encontrados:", test_files)

print(test_files)
module_strings = [test_file[0:len(test_file)-3] for test_file in test_files]
suites = [unittest.defaultTestLoader.loadTestsFromName(test_file) for test_file in module_strings]
test_suite = unittest.TestSuite(suites)
test_runner = unittest.TextTestRunner().run(test_suite)"""