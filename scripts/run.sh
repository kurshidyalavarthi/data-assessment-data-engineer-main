docker run \
  -v $(pwd):/code \
  -v $(pwd)/gen:/gen \
  -it \
  data-assessment-data-engineer "$@"