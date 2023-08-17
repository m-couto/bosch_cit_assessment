# Bosch CIT Assessment

This repository contains my solution to the technical task proposed by Bosch - CIT (connected innovation topics).

Here is an explanation of the files:
- 'data_acquisition.py': running this file will create the datasets folder and subfolders and download the datasets into them.
- 'data_processing.py': the function `process_data` performs the initial data preprocessing, which includes tasks such as removing duplicate data, handling missing data, fixing typos and removing outliers.
- 'data_transformation.py': the function `transform_data` performs further processing on the data, mainly preparing it into a format that can be used for modelling, that is converting all features into numerical.
- 'data_loading.py': a script for loading a dataset into a SQL Server.
- The datasets folder contains the datasets required for this challenge, as well as their respective processed and transformed versions.
- 'bosch_task_presentation.pptx': a Power Point presentation on this task.
