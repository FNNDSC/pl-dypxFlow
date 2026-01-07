# A dynamic ChRIS plugin for PACS

[![Version](https://img.shields.io/docker/v/fnndsc/pl-dypxflow?sort=semver)](https://hub.docker.com/r/fnndsc/pl-dypxflow)
[![MIT License](https://img.shields.io/github/license/fnndsc/pl-dypxFlow)](https://github.com/FNNDSC/pl-dypxFlow/blob/main/LICENSE)
[![ci](https://github.com/FNNDSC/pl-dypxFlow/actions/workflows/ci.yml/badge.svg)](https://github.com/FNNDSC/pl-dypxFlow/actions/workflows/ci.yml)

`pl-dypxFlow` is a [_ChRIS_](https://chrisproject.org/) **dynamic plugin** that orchestrates a PACS-based anonymization and processing pipeline.  
It interacts with **Orthanc / PACS**, **pfdcm**, and **ChRIS/CUBE** services to automate DICOM workflows.

## Abstract

`pl-dypxFlow` is designed to drive an anonymization and data-flow pipeline starting from a PACS source.  
The plugin supports:

- Pattern-based input file selection
- Integration with **CUBE/ChRIS**
- Interaction with **PACS / Orthanc**
- Optional parallel execution
- Optional wait-for-completion behavior
- Email notification support

This plugin is typically used as a **controller/orchestrator** rather than a pure data-transform plugin.

## Installation

`pl-dypxFlow` is a _[ChRIS](https://chrisproject.org/) plugin_, meaning it can
run from either within _ChRIS_ or the command-line.

## Local Usage

To get started with local command-line usage, use [Apptainer](https://apptainer.org/)
(a.k.a. Singularity) to run `pl-dypxFlow` as a container:

```shell
apptainer exec docker://fnndsc/pl-dypxFlow dypxFlow [--args values...] input/ output/
```

To print its available options, run:

```shell
apptainer exec docker://fnndsc/pl-dypxFlow dypxFlow --help
```

## Examples

`dypxFlow` requires two positional arguments: a directory containing
input data, and a directory where to create output data.
First, create the input directory and move input data into it.

```shell
mkdir incoming/ outgoing/
mv some.dat other.dat incoming/
apptainer exec docker://fnndsc/pl-dypxFlow:latest dypxFlow [--args] incoming/ outgoing/
```

## Development

Instructions for developers.

### Building

Build a local container image:

```shell
docker build -t localhost/fnndsc/pl-dypxFlow .
```

### Running

Mount the source code `dypxFlow.py` into a container to try out changes without rebuild.

```shell
docker run --rm -it --userns=host -u $(id -u):$(id -g) \
    -v $PWD/dypxFlow.py:/usr/local/lib/python3.12/site-packages/dypxFlow.py:ro \
    -v $PWD/in:/incoming:ro -v $PWD/out:/outgoing:rw -w /outgoing \
    localhost/fnndsc/pl-dypxFlow dypxFlow /incoming /outgoing
```

### Testing

Run unit tests using `pytest`.
It's recommended to rebuild the image to ensure that sources are up-to-date.
Use the option `--build-arg extras_require=dev` to install extra dependencies for testing.

```shell
docker build -t localhost/fnndsc/pl-dypxFlow:dev --build-arg extras_require=dev .
docker run --rm -it localhost/fnndsc/pl-dypxFlow:dev pytest
```

## Release

Steps for release can be automated by [Github Actions](.github/workflows/ci.yml).
This section is about how to do those steps manually.

### Increase Version Number

Increase the version number in `setup.py` and commit this file.

### Push Container Image

Build and push an image tagged by the version. For example, for version `1.2.3`:

```
docker build -t docker.io/fnndsc/pl-dypxFlow:1.2.3 .
docker push docker.io/fnndsc/pl-dypxFlow:1.2.3
```

### Get JSON Representation

Run [`chris_plugin_info`](https://github.com/FNNDSC/chris_plugin#usage)
to produce a JSON description of this plugin, which can be uploaded to _ChRIS_.

```shell
docker run --rm docker.io/fnndsc/pl-dypxFlow:1.2.3 chris_plugin_info -d docker.io/fnndsc/pl-dypxFlow:1.2.3 > chris_plugin_info.json
```

Intructions on how to upload the plugin to _ChRIS_ can be found here:
https://chrisproject.org/docs/tutorials/upload_plugin

