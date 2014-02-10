#!/usr/bin/env ruby

require 'sequel'
require 'rfusefs'

module Idea1
  extend self

  DB = Sequel.connect("postgres:///KitFS_idea1")

  COMMANDS = [
    [:put,    ["relpath"], [],
      -> (*args, &cmd) { cmd.(*(args + [$stdin.read])) }],

    [:get,    ["relpath"], [],
      -> (*args, &cmd) { $stdout << cmd.(*args) }],

    [:link,   ["srcpath", "destpath"], []],

    [:unlink, ["relpath"], []],

    [:list,   [], ["relpath"]],

    [:mount,  ["directory"], []]
  ]

  def locate(path, origin=0)
    if path.is_a? String
      path = path.split("/")
    end

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

  def put(relpath, input)
    contents = !input.nil? ? Sequel.blob(input) : nil

    begin
      # try to replace existing file
      file_id = locate(relpath.split("/"))

      DB[:files].filter(id: file_id).update(contents: contents)
    rescue
      # create new file and link
      parent_path = relpath.split("/")
      name        = parent_path.pop

      parent = locate(parent_path)

      DB.transaction do
        file = DB[:files].insert(contents: contents)

        DB[:relations].insert(source: parent, name: name, destination: file)
      end
    end
  end

  def get(relpath)
    file_id = locate(relpath)

    c = DB[:files].filter(id: file_id).select_map(:contents).first

    if c.nil?
      ""
    else
      c
    end
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
      file_id = locate(relpath)
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
    srcfile_id = locate(srcpath)

    parent_path = destpath.split("/")
    name        = parent_path.pop

    parent = locate(parent_path)

    DB[:relations].insert(source: parent, name: name, destination: srcfile_id)

    return 0
  end

  def unlink(relpath)
    parent_path = relpath.split("/")
    name        = parent_path.pop

    parent = locate(parent_path)

    if relation = DB[:relations][source: parent, name: name]
      DB[:relations].filter(id: relation[:id]).delete

      # TODO: cleanup
    else
      raise "relation '#{name}' not found on '#{parent_path.join("/")}'"
    end
  end

  class FS < FuseFS::FuseDir
    def contents(path)
      file_id = Idea1.locate(from_dir_path(path))

      DB[:relations].filter(source: file_id).select_map(:name)
    end

    def directory?(path)
      begin
        Idea1.locate(from_dir_path(path))
        true
      rescue
        false
      end
    end

    def file?(path)
      if path =~ /^(.*)=$/
        directory? $1
      else
        false
      end
    end

    def read_file(path)
      Idea1.get(from_path(path))
    end

    def size(path)
      file_id = Idea1.locate(from_path(path))

      DB[:files].filter(id: file_id)
        .select_map(Sequel.function(:octet_length, :contents)).first
    end

    def can_write?(path)
      directory?(path.split("/")[0..-2].join("/"))
    end

    def write_to(path, body)
      Idea1.put(from_path(path), body)
    end

    def can_delete?(path)
      file?(path) || directory?(path)
    end

    def delete(path)
      Idea1.unlink(from_path(path))
    end

    def can_mkdir?(path)
      !file?(path) && directory?(path.split("/")[0..-2].join("/"))
    end

    def mkdir(path)
      Idea1.put(from_dir_path(path), nil)
    end

    def can_rmdir?(path)
      directory?(path)
    end

    def rmdir(path)
      Idea1.unlink(from_dir_path(path))
    end

    private

    def from_dir_path(path)
      path.sub(/^\//, '')
    end

    def from_path(path)
      from_dir_path(path).sub(/=$/, '')
    end
  end

  def mount(directory)
    if File.directory?(directory)
      fork do
        Process.setsid
        FuseFS.set_root(FS.new)
        FuseFS.mount_under(directory)
        FuseFS.run
      end
    else
      raise "no such directory: '#{directory}'"
    end
  end
end

def interpret(command, *args)
  if c = Idea1::COMMANDS.find { |c| c[0].to_s == command.to_s }
    command_name, required, optional, block = c

    if args.count >= required.count and
       args.count <= required.count + optional.count

      block ||= -> (*args, &cmd) { cmd.(*args) }

      block.(*args) { |*new_args|
        Idea1.send(command_name, *new_args)
      }
      return 0
    else
      param_list = [command_name] + required.map { |o| "<#{o}>" }\
                                  + optional.map { |o| "[<#{o}>]" }
      warn "Usage: #$0 #{param_list.join(" ")}"
      return 1
    end
  else
    warn "Unrecognized command."
    return 1
  end
rescue
  warn "Error: #$!"
  return 1
end

if ARGV[0]
  exit(interpret(*ARGV))
else
  warn "Usage: #$0"

  Idea1::COMMANDS.each do |c|
    command_name, required, optional, block = c
    param_list = [command_name] + required.map { |o| "<#{o}>" }\
                                + optional.map { |o| "[<#{o}>]" }
    warn "         #{param_list.join(" ")}"
  end

  exit 1
end
