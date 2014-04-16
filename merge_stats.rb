

tot_terms = 0
ctfs = Hash.new(0)

for file in ARGV
  File.open(file) do |f|
    # first line is total term count in index
    line = f.gets
    tot_terms += line.strip.to_i

    while line = f.gets
      term, count = line.strip.split("\t")
      ctfs[term] += count.to_i
    end
  end
end

puts tot_terms
for term, count in ctfs
  puts "#{term}\t#{count}"
end
