FROM jupyter/pyspark-notebook:latest
CMD ["start-notebook.sh", "--NotebookApp.token=''", "--allow-root"]