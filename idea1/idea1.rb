#!/usr/bin/env ruby

require 'sequel'

DB = Sequel.connect("postgres:///KitFS_idea1")

def locate(path, origin=0)
  path.each_with_index do |relation_name, index|
    origin = DB[:relations].filter(source: origin, name: relation_name)
                           .select_map(:destination).first

    if origin.nil?
      raise "relation '#{relation_name}' not found on '#{
        path[0, index].join("/")}'"
    end
  end

  return origin
end

def put(relpath)
  parent_path = relpath.split("/")
  name        = parent_path.pop

  begin
    parent = locate(parent_path)
  rescue
    warn "Error: #$!"
    return 1
  end

  file = DB[:files].insert(contents: Sequel.blob($stdin.read))

  DB[:relations].insert(source: parent, name: name, destination: file)

  return 0
end

def get(relpath)
  begin
    file_id = locate(relpath.split("/"))
  rescue
    warn "Error: #$!"
    return 1
  end

  $stdout << DB[:files].filter(id: file_id).select_map(:contents).first

  return 0
end

LIST_QUERY = <<-SQL
SELECT
  r1.destination AS file_id,
  count(r2.id) AS subrelations,
  octet_length(f.contents) AS bytes,
  r1.name AS name
FROM
  relations AS r1
INNER JOIN
  files AS f ON f.id = r1.destination
LEFT OUTER JOIN
  relations AS r2 ON r2.source = r1.destination
WHERE
  r1.source = ?
GROUP BY
  r1.name,
  r1.destination,
  f.contents
ORDER BY
  r1.name ASC;
SQL

def list(relpath="")
  begin
    file_id = locate(relpath.split("/"))
  rescue
    warn "Error: #$!"
    return 1
  end

  headings = ["file ID", "subrelations", "bytes", "name"]

  printf "%-15s %-15s %-15s %s\n", *headings

  printf "%-15s %-15s %-15s %s\n", *headings.map {|s| s.gsub(/./, "-")}

  results = DB[LIST_QUERY, file_id].all
  
  results.each do |relation|
    printf "%-15s %-15s %-15s %s\n",
      relation[:file_id], relation[:subrelations],
      relation[:bytes], relation[:name]
  end

  puts
  puts "Total: #{results.count} relations"

  return 0
end

def link(srcpath, destpath)
  begin
    srcfile_id = locate(srcpath.split("/"))
  rescue
    warn "Error: #$!"
    return 1
  end

  parent_path = destpath.split("/")
  name        = parent_path.pop

  begin
    parent = locate(parent_path)
  rescue
    warn "Error: #$!"
    return 1
  end

  DB[:relations].insert(source: parent, name: name, destination: srcfile_id)

  return 0
end

def unlink(relpath)
  parent_path = relpath.split("/")
  name        = parent_path.pop

  begin
    parent = locate(parent_path)
  rescue
    warn "Error: #$!"
    return 1
  end

  if relation = DB[:relations][source: parent, name: name]
    DB[:relations].filter(id: relation[:id]).delete

    # TODO: cleanup
  else
    warn "Error: relation '#{name}' not found on '#{parent_path.join("/")}'"
    return 1
  end
end

def interpret(command, *args)
  case command
  when "put", "get", "unlink"
    if args.count == 1
      return send(command, *args)
    else
      warn "Usage: #$0 #{command} <relpath>"
      return 1
    end
  when "link"
    if args.count == 2
      return link(*args)
    else
      warn "Usage: #$0 link <srcpath> <destpath>"
    end
  when "list"
    return list(*args)
  else
    warn "Unrecognized command."
    return 1
  end
end

if ARGV[0]
  exit(interpret(*ARGV))
else
  warn "Usage: #$0",
       "         put    <relpath>",
       "         get    <relpath>",
       "         link   <relpath>",
       "         unlink <relpath>",
       "         list   <relpath>"

  exit 1
end
