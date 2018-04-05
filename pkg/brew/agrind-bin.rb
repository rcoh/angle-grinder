class AgrindBin < Formula
  version 'v0.7.1'
  desc "Slice and dice log files on the command line"
  homepage "https://github.com/rcoh/angle-grinder"

  if OS.mac?
      url "https://github.com/rcoh/angle-grinder/releases/download/#{version}/angle_grinder-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "be280ed87b044438fe20a87ceb2be96930ccab7ebf7b1ac80385abd8edb19b53"
  elsif OS.linux?
      url "https://github.com/rcoh/angle-grinder/releases/download/#{version}/angle_grinder-#{version}-x86_64-unknown-linux-musl.tar.gz"
      sha256 "5ce30c62d090e8ca0c71d5a3f10a8ff46363d67d2c651981cb8d18a7d93dcf77"
  end


  def install
    bin.install "agrind"
  end
end
