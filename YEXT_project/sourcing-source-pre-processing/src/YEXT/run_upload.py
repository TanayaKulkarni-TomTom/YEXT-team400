from poi_loader.upload_yext_ftp import poi_dl_upload
from definitions import PRE_PROCESSED_DIR
from pathlib import Path
import traceback


def main():
    try:
        folder = Path(PRE_PROCESSED_DIR)
        files = [x for x in folder.iterdir()]
        for file in files:
            try:
                poi_dl_upload([file])
            except Exception as e:
                print(
                    f"Error when sending a file to poi loader --- {file} --- ",
                    traceback.format_exc(limit=1),
                )

    except Exception as e:
        print(
            f"Error when uploading files to POI Loader: {e} --- ",
            traceback.format_exc(limit=1),
        )


if __name__ == "__main__":
    main()
