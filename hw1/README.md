# Week 1

## Prerequisites

* docker
* docker-compose

## Usage

Run the following commands to see the answers to homework questions:

```shell
# Question 1
docker build --help | grep "Write the image ID to the file"

# Question 2
docker run --rm -it python:3.9 /bin/bash
pip list | tail -n +3 | wc -l

# Questions 3-6
docker-compose up --build
```
