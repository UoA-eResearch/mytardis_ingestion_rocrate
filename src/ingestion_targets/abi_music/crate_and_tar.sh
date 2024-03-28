#!/usr/bin/env bash
TARGET_DIR=$1
TAR_OUTPUT=$2
echo "##"
echo "#Building Crates for" $TARGET_DIR
echo "##"
ro_crate_builder abi -i $TARGET_DIR
echo "##"
echo "#Building Archives for bagged crates in" $TARGET_DIR
echo "##"
find $TARGET_DIR -name 'bagit.txt' -printf '%h\n' |sort -u| xargs -t -I {} tar \
 --exclude=**/{*.AppleDB,*.Trashes,.*,*.DS_Store,*.DS_store,*.AppleDouble,*.TemporaryItems,*.Spotlight-V100,*.vol,*.fseventsd,*._*,*.FileSync-lock,*.com.apple.timemachine.donotpresent,*.fseventsd} \
 --exclude-caches --exclude-backups -cvzf {}.tar.gz {};
find $TARGET_DIR -name '*.tar.gz' -print | xargs -t -I {} mv {} $TAR_OUTPUT
