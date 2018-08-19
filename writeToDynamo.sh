#!/nix/store/pw59d08i9fyn9rj6w3yqk0gkc54i5ccz-system-path/bin/bash
#!/bin/bash

declare tn="vocab-test-table"

# declare header='{"'${tn}'": ['
declare header='{"RequestItems":{"'${tn}'": ['

# declare footer=']}'
declare footer=']}}'

for filename in ./models/*; do
  while mapfile -t -n 10 ary && ((${#ary[@]})); do
      content="${ary[@]}"
      upload="$header"${content::-1}"$footer"
      aws dynamodb batch-write-item --region us-east-1 --cli-input-json "$upload"
      #echo $upload
  done < $filename
done
