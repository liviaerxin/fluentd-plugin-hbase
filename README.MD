HBase output plugin buffers event logs in local file and puts it to HBase periodically.

Previuos Work
Install massive_record,
sudo git clone https://github.com/CompanyBook/massive_record.git
sudo gem install activemodel -v=3.0.19
sudo gem install massive_record-0.2.3.gem
sudo gem install activemodel -v=3.0.20
sudo gem install thrift
sudo gem install tzinfo

cd massive_record
Under the local massive_record directory,
gem build massive_record.gemspec
sudo gem install massive_record
That will search the local *.gem and install from local.
