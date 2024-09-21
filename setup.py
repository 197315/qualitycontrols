from setuptools import setup, find_packages

setup(
    name='qualitycontrols',              # Nombre de tu librería
    version='0.1.0',                 # Versión de la librería
    packages=find_packages(),        # Encuentra automáticamente los paquetes
    install_requires=[               # Lista de dependencias
        # Añade aquí las dependencias que requiera tu librería
    ],
    author='Marcelo Gabriel Gonzalez',              # Autor del paquete
    author_email='marcelo.g.gonzalez95@gmail.com',  # Correo del autor
    description='Descripción breve de tu librería',
    long_description=open('README.md').read(),  # Descripción larga (de README.md)
    long_description_content_type='text/markdown',
    url='https://github.com/197315/qualitycontrols',  # URL de tu proyecto (GitHub o similar)
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Cambia la licencia si es necesario
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',         # Versión mínima de Python
)
