from setuptools import setup, find_packages
import os

def get_requirements(directory):
    requirements_file = os.path.join(directory, 'requirements.txt')
    if os.path.exists(requirements_file):
        with open(requirements_file) as f:
            return f.read().splitlines()
    return []

# Globale Requirements
global_requirements = get_requirements('.')

# Modulspezifische Requirements
submodules = ['document_safe', 'document_safe/document_loader']
all_requirements = global_requirements

for submodule in submodules:
    all_requirements.extend(get_requirements(submodule))

# Entfernen von Duplikaten
all_requirements = list(set(all_requirements))

setup(
    name='document_safe',
    version='0.1',
    packages=find_packages(exclude=['tests', 'docs']),
    install_requires=all_requirements,
    # Weitere Metadaten hier...
)
