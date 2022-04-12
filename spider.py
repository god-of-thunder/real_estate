import requests
target_filename_list=["A","B","E","F","H"]
for target_filename in target_filename_list:

    url = 'https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName={}_lvr_land_A.csv'.format(target_filename)
    file = requests.get(url, allow_redirects=True)

    open('{}_lvr_land_A.csv'.format(target_filename), 'wb').write(file.content)