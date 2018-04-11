class AgrindBin < Formula
  version 'v0.7.3'
  desc "Slice and dice log files on the command-line"
  homepage "https://github.com/rcoh/angle-grinder"

  if OS.mac?
      url "https://github.com/rcoh/angle-grinder/releases/download/#{version}/angle_grinder-#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "d0682656294bc5d764d4110ae93add1442f6420bff49dcc4d49cb635950014fb"
  elsif OS.linux?
      url "https://github.com/rcoh/angle-grinder/releases/download/#{version}/angle_grinder-#{version}-x86_64-unknown-linux-musl.tar.gz"
      sha256 "6dc5438443e653e5d893c6858d0f9279205728314292636c7b5b18706a9aa759"
  end


  def install
    bin.install "agrind"
  end
end
