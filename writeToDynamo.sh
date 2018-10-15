#!/nix/store/pw59d08i9fyn9rj6w3yqk0gkc54i5ccz-system-path/bin/bash
#!/bin/bash

declare tn=$1
# declare tn="vocab-test-table"
declare directory=$2
# declare directory="./models"
declare x=$3
# declare x=10

# declare header='{"'${tn}'": ['
declare header='{"RequestItems":{"'${tn}'": ['

# declare footer=']}'
declare footer=']}}'

for filename in $directory/*; do
  while mapfile -t -n $x ary && ((${#ary[@]})); do
      sleep 0.5
      content="${ary[@]}"
      upload="$header"${content::-1}"$footer"
      aws dynamodb batch-write-item --region us-east-1 --cli-input-json "$upload"
      #echo $upload
  done < $filename
done
