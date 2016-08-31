package org.pentaho.big.data.impl.spark;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ArchiveInstaller {
  private final byte[] buffer = new byte[ 1024 ];
  private final ArchiveStreamFactory archiveStreamFactory = new ArchiveStreamFactory();


  File install( InputStream archiveStream, File installDir ) throws IOException {
    try ( ArchiveInputStream zip = archiveStreamFactory.createArchiveInputStream( archiveStream ) ) {
      ArchiveEntry entry = zip.getNextEntry();
      if ( entry.isDirectory() ) {
        File sparkDir = new File( installDir, entry.getName() );
        sparkDir.mkdirs();
        unpack( zip, installDir );
        return sparkDir;
      } else if ( entry.getName().endsWith( ".tar" ) ) {
        return install( zip, installDir );
      } else {
        throw new RuntimeException(
          "Expected a top-level directory in spark archive, instead found " + entry.getName()
        );
      }
    } catch ( ArchiveException e ) {
      throw new IOException( e );
    }
  }

  private void unpack( ArchiveInputStream zip, File installDir ) throws IOException {
    for ( ArchiveEntry entry = zip.getNextEntry(); entry != null; entry = zip.getNextEntry() ) {
      File output = new File( installDir, entry.getName() );
      if ( entry.isDirectory() ) {
        output.mkdirs();
      } else {
        try ( OutputStream outputStream = new FileOutputStream( output ) ) {
          unpack( zip, outputStream, entry.getSize() );
        } catch ( Exception e ) {
          throw new IOException( "Error unpacking " + output, e );
        }
      }
    }
  }

  private void unpack( InputStream inputStream, OutputStream outputStream, long remaining )
    throws IOException {
    Preconditions.checkArgument( remaining >= 0, "remaining bytes must be greater than 0" );

    if ( remaining != 0 ) {
      int read = inputStream.read( buffer, 0, (int) Math.min( buffer.length, remaining ) );
      Preconditions.checkState( read > 0, "No more data from archive, expecting {0} more bytes", remaining );

      outputStream.write( buffer, 0, read );
      unpack( inputStream, outputStream, remaining - read );
    }
  }
}