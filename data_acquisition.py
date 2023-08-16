import os
import requests
import zipfile


dataset_folder_name = 'datasets'

# retrieving my personal API key for datasets in developer.nrel
# (required for Alternative Fuel Stations dataset)
with open('api_key.txt') as f:
    api_key = f.readlines()
api_key = api_key[0]

dataset_links = {
    # 'nhtsa': 'https://www.nhtsa.gov/nhtsa-datasets-and-apis',
    'dep_energy': (
        'https://developer.nrel.gov/api/alt-fuel-stations/v1.csv?api_key=' + api_key,
        'alt_fuel_stations.csv'
    ),
    'epa': (
        'https://www.fueleconomy.gov/feg/epadata/vehicles.csv',
        'vehicles.csv'
    )
}


if __name__=='__main__':

    # create datasets folder
    if not os.path.exists(dataset_folder_name):
        os.mkdir(dataset_folder_name)
        print('Created folder for data successfully')

    for source, (url_link, filename) in dataset_links.items():

        # create directory for datasets from this source
        if not os.path.exists(dataset_folder_name + '/' + source):
            os.mkdir(dataset_folder_name + '/' + source)
            print(f'Created subfolder for {source} data successfully')
        
        # download data into source folder
        filepath = dataset_folder_name + '/' + source + '/' + filename
        response = requests.get(url_link)
        open(filepath, "wb").write(response.content)
        print('File downloaded successfully:', filename)

        # if the file is zipped, unzip it
        if filename.endswith('.zip'):
            with zipfile.ZipFile(filepath, 'r') as zip_ref:
                zip_ref.extractall(dataset_folder_name + '/' + source)

            filename = filename[:-4]
            print('File unzipped successfully:', filename)
