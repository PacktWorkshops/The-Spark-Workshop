:paste
gpa
  .cube($"school", $"class")
  .count()
  .sort(asc("school"), asc("class"))
  .show()

:paste
gpa
  .rollup($"school", $"class")
  .count()
  .sort(asc("school"), asc("class"))
  .show()
