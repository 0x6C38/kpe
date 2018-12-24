#!/bin/bash
# #!/nix/store/pw59d08i9fyn9rj6w3yqk0gkc54i5ccz-system-path/bin/bash

declare tn=$1
# declare tn="kanjiapp-dev-master"
declare directory=$2
# declare directory="./output-aws-word-entries"
declare x=$3
# declare x=10

# declare header='{"'${tn}'": ['
declare header='{"RequestItems":{"'${tn}'": ['

# declare footer=']}'
declare footer=']}}'

SECONDS=0

for filename in $directory/*; do
  while mapfile -t -n $x ary && ((${#ary[@]})); do
      sleep 0.5
      content="${ary[@]}"
      upload="$header"${content::-1}"$footer"
      aws dynamodb batch-write-item --region us-east-1 --cli-input-json "$upload"
      #echo $upload

      counter=$((counter+x))
      printf "Processed: ${counter}"
      echo ""
  done < $filename
done

duration=$SECONDS
echo "$(($duration / 60 / 60)) hours $(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed."


