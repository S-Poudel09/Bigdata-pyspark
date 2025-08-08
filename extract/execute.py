import os,sys,requests
from zipfile import ZipFile

def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok = True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "download.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded zip file : {filename}")
        return filename
    else:
        raise Exception(f"Failed to downloas file Status code {response.status_code}")
    
def extract_zip_file(zip_filename, output_dir):

    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)
    
    print(f"Extracted files written to: {output_dir}")
    print("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(output_dir):
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    with open(file_path, "r") as f:
        data = json.load(f)

    with open(os.path.join(output_dir,"fixed_da.json"), "w", encoding="utf-8")as f_out:
        for key, value in data.items():
            record = {"id": key, "releated_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
        print (f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json")
    print("Removing the original file")
    os.remove(file_path)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Exam Usage:")
        print("python3 execute.py /home/princess/Data/Extraction")
    else:
        try:
            print("Starting Extraction Engine..,")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250725%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250725T022441Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=3216293a8fb187aa2b43a0d9f7d646be57ab69910c7eabd5c9b00683b0f6863033cd5b419524be726243f2efddff8f04c4b593c82833ee795b8fef19a07b2fe017dd3d3222fdb4eb40bd102b83911cf15c24e1d0d134f4f824285be5686064ee1be2b36d4b29eebc248bdd6705439cb86288e103848065a49fb6829b4d851e04171e2fee35d587adaf982a089d08dddadfea71784e45da9fd00e936f15d64e532824a7fde99421f2e47e416956aa183e52aac8af982555917df1e8e64f38ace47bb3487fa87f288fe96078e241a3af63b67e99638e340506830fca1b30d371c00d49302635561a7daff5f54754eb4f1d4ed2c8c492dc4b007738be4af5b2a2d1"
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(zip_filename, EXTRACT_PATH)
            fix_json_dict(EXTRACT_PATH)
            print("Extraction Successfully Completed!!!")
        except Exception as e:
            print (f"Error: {e}")
