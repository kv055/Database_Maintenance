FROM python3.8

WORKDIR /AbelianDatabaseMaintenance
COPY . .
RUN pip install -r req.txt

CMD ["python3","App.py"]

