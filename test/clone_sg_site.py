import os
import shotgun_api3
import pickle
from pathlib import Path
import requests

sg = shotgun_api3.Shotgun(
    os.environ["SG_SERVER"],
    os.environ["SG_SCRIPT_NAME"],
    os.environ["SG_SCRIPT_KEY"],
)

save_dir = Path(__file__).parent

schema_file = save_dir / "schema.pickle"
schema_entity_file = save_dir / "schema_entity.pickle"
entity_dump_folder = save_dir / "entity_dumps"
if not entity_dump_folder.exists():
    entity_dump_folder.mkdir()

schema_entity = sg.schema_entity_read()
schema = sg.schema_read()

pickle.dump(schema, open(schema_file, "wb"))
pickle.dump(schema_entity, open(schema_entity_file, "wb"))

for entity_type, fields in schema.items():
    print(f"Dumping {entity_type}...")
    try:
        entity_dump = sg.find(entity_type, [], list(fields))
    except Exception as e:
        print(f"Error: {e}")
        continue
    pickle.dump(entity_dump, open(entity_dump_folder / f"{entity_type}.pickle", "wb"))
    # download attachments
    for field_name, field_data in fields.items():
        if field_data["data_type"]["value"] in ["image"]:
            attachment_folder = entity_dump_folder / entity_type / field_name
            if not attachment_folder.exists():
                attachment_folder.mkdir(parents=True)
            print(f"Downloading {field_name}...\n")
            for item in entity_dump:
                if field_name in item and item[field_name]:
                    url = (
                        item[field_name]["url"]
                        if isinstance(item[field_name], dict)
                        else item[field_name]
                    )
                    if url and "http" in url:
                        print("\rDownloading", url[:50], end="")
                        r = requests.get(url, allow_redirects=True)
                        open(
                            attachment_folder
                            / f"{entity_type}_{field_name}_{item['id']}",
                            "wb",
                        ).write(r.content)
    print(f"Done!")
