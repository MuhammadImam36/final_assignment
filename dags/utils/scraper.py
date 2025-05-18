import requests
import pandas as pd

class BPNScraper():
    def __init__(self, url, params):
        self.url = url
        self.params = params

    def get_response(self):
        response = requests.get(self.url, params = self.params)
        return response.json()

    def metadata_status(self):
        # Variable definition
        data_status = self.get_response().get('detail').get('status')
        source_columns = ['status_aman', 'status_waspada', 'status_intervensi']
        columns_df = ['deskripsi_status']

        # Scrape_logic
        value = [[data_status.get(key)] for i, key in enumerate(source_columns)]
        value_df = pd.DataFrame(value, columns = columns_df)
        return value_df.to_csv("/opt/airflow/data/metadata_status.csv", index=False)

    def metadata_provinsi(self):
        # Variable definition
        url_provinsi = "https://api-panelhargav2.badanpangan.go.id/api/provinces?search="
        source_columns = ['id', 'nama', 'is_produsen']
        columns_df = ['id_provinsi', 'nama_provinsi', 'is_produsen']
        data_province = requests.get(url_provinsi).json().get('data')

        # Scrape_logic
        data = [list(data.get(column) for column in source_columns) for data in data_province]
        data_df = pd.DataFrame(data = data, columns = columns_df)
        return data_df.to_csv("/opt/airflow/data/metadata_provinsi.csv", index=False)

    def metadata_komoditas(self):
        # Variable definition
        url_komoditas = "https://api-panelhargav2.badanpangan.go.id/api/cms/eceran"
        source_columns = ['id', 'nama', 'secctor_id_desc']
        columns_df = ['id_komoditas', 'komoditas', 'nama_sektor']
        data_komoditas = requests.get(url_komoditas).json().get('data')

        # Scrape_logic
        data = [list(data.get(column) for column in source_columns) for data in data_komoditas]
        data_df = pd.DataFrame(data, columns = columns_df)
        return data_df.to_csv("/opt/airflow/data/metadata_komoditas.csv", index=False)

    def data_harga_provinsi(self):
        # Variable definition
        data_harian = self.get_response().get('data')
        source_columns = ['date', 'province_id', 'commodity_id', 'rata_rata_geometrik', 'hpp_hap', 'hpp_hap_percentage', 'map_color']
        df_column = ['date', 'id_provinsi', 'id_komoditas', 'harga_rata_rata', 'het', 'disparitas_het', 'status']

        # Scrape_logic
        data = [[data.get(column) for column in source_columns] for data in data_harian]
        return pd.DataFrame(data, columns = df_column)

    def data_harga_nasional(self):
        # Variable definition
        source_columns_1 = ['id', 'komoditas_id']
        source_columns_2 = ['hargaratarata', 'map_color', 'hpp_hap', 'hpp_hap_percentage']
        columns_df = ['id_provinsi', 'id_komoditas', 'harga_rata_rata', 'status', 'het', 'disparitas_het', 'date']
        setting_harga = self.get_response().get('request_data').get('setting_harga')
        data_detail = self.get_response().get('detail')
        date = self.get_response().get('data')[0].get('date')

        # Scrape_logic
        data_nasional1 = [list(data.get(column) for column in source_columns_1) for data in setting_harga[-1:]]
        data_nasional1 = data_nasional1[0]
        data_nasional2 = [data_detail.get(column) for column in source_columns_2]
        data_nasional = data_nasional1 + data_nasional2
        data_nasional.append(date)
        return pd.DataFrame([data_nasional], columns = columns_df)

    def data_harga_zona(self):
        # Variable definition
        columns_1 = ['id', 'komoditas_id', 'harga_provinsi']
        columns_2 = ['rata_rata', 'hpp_hap_percentage', 'hpp_hap_color_gap']
        columns_df = ['id_provinsi', 'id_komoditas', 'het', 'harga_rata_rata', 'disparitas_het', 'status', 'date']
        listt = []
        date = self.get_response().get('data')[0].get('date')
        setting_harga = self.get_response().get('request_data').get('setting_harga')
        zona = self.get_response().get('detail').get('zona')

        # Scrape_logic
        data1 = [list(data.get(column) for column in columns_1) for data in setting_harga[:-1]]
        data2 = [list(data.get(column) for column in columns_2) for data in zona]
        for i in range(3):
            tes = data1[i] + data2[i]
            tes.append(date)
            listt.append(tes)
        return pd.DataFrame(listt, columns = columns_df)
    
    def data_harga_harian(self):
        df_combined = pd.concat([self.data_harga_provinsi(), self.data_harga_zona(), self.data_harga_nasional()], ignore_index=True)
        return df_combined.to_csv("/opt/airflow/data/data_harga_harian.csv", index=False)