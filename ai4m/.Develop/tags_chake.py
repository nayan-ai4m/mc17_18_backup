from pycomm3 import LogixDriver

PLC_IP = '141.141.141.138'
SEARCH_KEYWORD = 'HMI_Hor_Seal_Rear_35'

with LogixDriver(PLC_IP) as plc:
    if plc.connected:
        all_tags = plc.get_tag_list()

        # Find tags that contain the search keyword
        matching_tags = [t['tag_name'] for t in all_tags if SEARCH_KEYWORD.lower() in t['tag_name'].lower()]

        if matching_tags:
            print(f"✅ Found tags matching '{SEARCH_KEYWORD}':")
            for tag in matching_tags:
                try:
                    tag_data = plc.read(tag)
                    if tag_data:
                        print(f" - {tag} = {tag_data.value}")
                    else:
                        print(f" - {tag} -> Failed to read value")
                except Exception as e:
                    print(f" - {tag} -> Error: {e}")
        else:
            print(f"❌ No tags found containing '{SEARCH_KEYWORD}'")

