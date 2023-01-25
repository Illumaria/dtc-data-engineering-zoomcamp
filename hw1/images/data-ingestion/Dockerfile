FROM python:3.9

RUN echo "Question 2: How many python packages/modules are installed?"
RUN echo "Answer: $(pip list | tail -n +3 | wc -l)"  # 3

RUN apt-get update --yes && apt-get install --yes wget

WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]
