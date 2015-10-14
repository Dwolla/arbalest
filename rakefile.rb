INSTALL = "install.txt"

desc "Clean"
task :clean do
    FileUtils.rm_rf("build")
    Dir["arbalest/**/*"].delete_if { |f| f.end_with?(".pyc") }
    Dir["test/**/*"].delete_if { |f| f.end_with?(".pyc") }
end

namespace :test do
    desc "Run unit tests"
    task :unit do
        sh "python -m pip install virtualenv --upgrade"
        sh "python -m virtualenv env"

        arch = `python -c \"import struct; print struct.calcsize(\'P\') * 8 \"`

        if arch.include? "64"
            sh "env/Scripts/python -m pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win64-py27#egg=psycopg2"
        elsif arch.include? "32"
            sh "env/Scripts/python -m pip install -e git+https://github.com/nwcell/psycopg2-windows.git@win32-py27#egg=psycopg2"
        end

        sh "env/Scripts/python setup.py test"
        FileUtils.rm_rf("env")
    end

    desc "Run integration tests"
    task :integration do
        sh "git submodule update --init"
        sh "vagrant up --provision"
        sh "vagrant destroy -f"
    end
end

desc "Install"
task :install do
  sh "python setup.py install --record #{INSTALL}"
end

desc "Uninstall"
task :uninstall do
  File.open("#{INSTALL}") do |f|
    File.readlines("#{INSTALL}").each { |file| sh "rm -rf #{file}" }
  end
  Rake::Task["clean"].invoke
end

task :default do
  Rake::Task["clean"].invoke
  Rake::Task["test:unit"].invoke
end
