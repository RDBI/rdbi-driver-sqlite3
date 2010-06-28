require 'rubygems'
require 'rake'

version = (File.exist?('VERSION') ? File.read('VERSION') : "").chomp

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "rdbi-dbd-sqlite3"
    gem.summary = %Q{sqlite3 driver for RDBI}
    gem.description = %Q{sqlite3 driver for RDBI}
    gem.email = "erik@hollensbe.org"
    gem.homepage = "http://github.com/erikh/rdbi-dbd-sqlite3"
    gem.authors = ["Erik Hollensbe"]

    gem.add_development_dependency 'test-unit'
    gem.add_development_dependency 'rdoc'

    gem.add_dependency 'rdbi'
    gem.add_dependency 'sqlite3-ruby'
    gem.add_dependency 'methlab'
    gem.add_dependency 'epoxy'

    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/test_*.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

require 'rdoc/task'
RDoc::Task.new do |rdoc|
  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "rdbi-dbd-sqlite3 #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end

task :install => [:test, :build]
