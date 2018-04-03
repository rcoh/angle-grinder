class AgrindBin < Formula
  version '0.6.3'
  desc "Slice and dice log files on the command line"
  homepage "https://github.com/rcoh/angle-grinder"

  if OS.mac?
      url "https://github.com/rcoh/angle-grinder/releases/download/#{version}/angle_grinder-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "e25ccca70d2d0ca63d997ef195083f18dadc1cd22220e73e8d49c5608d9f9a53"
  elsif OS.linux?
      url "https://github.com/rcoh/angle-grinder/releases/download/#{version}/angle_grinder-#{version}-x86_64-unknown-linux-musl.tar.gz"
      sha256 "291dc43f9da10f3311fa89635ebb1559b983cf217b40af292e199577f3cab2fb"
  end


  def install
    bin.install "agrind"
  end
end
