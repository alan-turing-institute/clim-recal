import sys
import pathlib
import re

# get input dir
root_dir = sys.argv[1]
root_dir = pathlib.Path(root_dir).resolve()
print(f"root_dir={root_dir}")

do_delete = False

try:
    print(f"secound_arg={sys.argv[2]}")
    if sys.argv[2] == "--I-am-really-sure-I-want-to-delete-lots-of-files":
        do_delete = True
except IndexError:
    pass

# There is certaining a better, more pythonic way to do this
# But I had already created and tested this regex, to work with ripgrep, before
# finding that it was too hard to install repgrep within the docker image
re_str = r"(\/resample\/(cpm|hads)\/.+\.nc|\/crops\/hads\/(?P<h_region>(Scotland|Glasgow|Manchester|London))\/(?P<h_var>(tasmin|tasmax|rainfall))\/crop_(?P=h_region)_(?P=h_var)_hads_\d{8}-\d{8}\.nc|\/crops\/cpm\/(?P<c_region>(Scotland|Glasgow|Manchester|London))\/(?P<c_var>(tasmin|tasmax|pr))\/(?P<emsb>(01|05|06|07|08))\/crop_(?P=c_region)_(?P=c_var)_cpm_(?P=emsb)_\d{8}-\d{8}\.nc)"

find_valid_files = re.compile(re_str)

i_kept = 0
i_deleted = 0

for root, dirs, files in root_dir.walk(top_down=True):
    for name in files:
        full_name = (root / name).resolve()

        if not find_valid_files.search(str(full_name)):
            # print(f"delete={full_name}")
            i_deleted += 1
            if do_delete:
                full_name.unlink()
        else:
            # print(f"keep={full_name}")
            i_kept += 1

print(f"found {i_deleted} files could be deleted and {i_kept} that should be kept")
