import json
import os
import requests
from modelscope import snapshot_download

if __name__ == '__main__':
    mineru_patterns = [
        "models/Layout/LayoutLMv3/*",
        "models/Layout/YOLO/*",
        "models/MFD/YOLO/*",
        "models/MFR/unimernet_small_2501/*",
        "models/TabRec/TableMaster/*",
        "models/TabRec/StructEqTable/*",
    ]
    model_dir = snapshot_download(
        "opendatalab/PDF-Extract-Kit-1.0",
        allow_patterns=mineru_patterns,
        local_dir="/opt/",
    )

    layoutreader_model_dir = snapshot_download(
        "ppaanngggg/layoutreader",
        local_dir="/opt/layoutreader/",
    )

    model_dir = model_dir + "/models"
    print(f"model_dir is: {model_dir}")
    print(f"layoutreader_model_dir is: {layoutreader_model_dir}")