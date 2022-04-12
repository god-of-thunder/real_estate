import requests
# repalce target csv of filename list
target_filename_list=["A","B","E","F","H"]
for target_filename in target_filename_list:
    # request target url to get file
    url = 'https://plvr.land.moi.gov.tw//DownloadSeason?season=108S2&fileName={}_lvr_land_A.csv'.format(target_filename)
    file = requests.get(url, allow_redirects=True)
    # save response to csv file
    open('{}_lvr_land_A.csv'.format(target_filename), 'wb').write(file.content)