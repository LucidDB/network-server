/*
Dynamo Network is a service provided by DynamoBI for managing LucidDB packages.
Copyright (C) 2011 Dynamo Business Intelligence Corporation

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the Free
Software Foundation; either version 2 of the License, or (at your option)
any later version approved by Dynamo Business Intelligence Corporation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
*/
package com.dynamobi.network;

import java.net.*;
import java.util.*;
import java.io.*;
import java.sql.*;
import java.security.MessageDigest;

import org.json.simple.*;
import org.json.simple.parser.*;

import net.sf.farrago.util.*; // Needed for $FARRAGO_HOME

/**
 * Provides a set of procedures and a function meant to communciate
 * with a server hosting luciddb packages in the form of jar files.
 *
 * @author Kevin Secretan
 */
public class DynamoNetworkUdr {

  /**
   * Procedure to avoid an explicit insert yourself, also takes off the ending slash
   * of a url/file-url if supplied.
   */
  public static void addRepo(String repoUrl) throws SQLException {
    if (repoUrl.endsWith("/")) {
      repoUrl = repoUrl.substring(-1);
    }
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    String query = "INSERT INTO localdb.sys_network.repositories (repo_url) "
      + "VALUES (?)";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setString(1, repoUrl);
    ps.execute();
    ps.close();
  }

  /**
   * Does the delete for you.
   */
  public static void removeRepo(String repoUrl) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    String query = "DELETE FROM localdb.sys_network.repositories WHERE "
      + "repo_url = ?";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.setString(1, repoUrl);
    ps.execute();
    ps.close();
  }


  /**
   * Downloads and saves a .jar into the plugins directory along with deps.
   */
  public static void download(String pkgName) throws SQLException {
    download(pkgName, false);
  }

  /**
   * Fetches and installs a package, with dependencies.
   */
  public static void install(String pkgName) throws SQLException {
    download(pkgName, true);
  }

  /**
   * Shows all available packages we have available.
   */
  public static void showPackages(PreparedStatement resultInserter)
    throws SQLException
  {
    List<String> repos = getRepoUrls();
    for (String repo : repos) {
      JSONObject repo_data = downloadMetadata(repo);
      JSONArray pkgs = (JSONArray) repo_data.get("packages");
      for (JSONObject obj : (List<JSONObject>)pkgs) {
        String status = getStatus(obj.get("jar").toString());
        int c = 0;
        resultInserter.setString(++c, repo);
        resultInserter.setString(++c, obj.get("type").toString());
        resultInserter.setString(++c, obj.get("publisher").toString());
        resultInserter.setString(++c, obj.get("package").toString());
        resultInserter.setString(++c, obj.get("version").toString());
        resultInserter.setString(++c, obj.get("jar").toString());
        resultInserter.setString(++c, status);
        resultInserter.executeUpdate();
      }
    }
  }

  /**
   * Deletes the .jar files and their rdeps from the plugins directory and from luciddb.
   */
  public static void remove(String pkgName) throws SQLException {
    remove(pkgName, true, true);
  }

  /**
   * Uninstalls jars from luciddb, leaves files around, takes care of reverse deps.
   */
  public static void uninstall(String pkgName) throws SQLException {
    remove(pkgName, false, true);
  }

  /**
   * Downloads the jar and their dep jars, optionally installing them too.
   */
  public static void download(String pkgName, boolean install)
    throws SQLException
  {
    List<String> repos = getRepoUrls();
    for (String repo : repos) {
      // by default, we pick the package from the first repo we find it in.
      // Enhancement: supply repo url in function call.
      JSONObject repo_data = downloadMetadata(repo);
      JSONArray pkgs = (JSONArray) repo_data.get("packages");
      for (JSONObject obj : (List<JSONObject>)pkgs) {
        if (pkgName.equals(obj.get("package").toString())) {
          fetchJar(repo, obj.get("jar").toString(), obj.get("md5sum").toString());
          if (install)
            installJar(obj.get("jar").toString());
          // automatically install dependencies, and the deps of deps,
          // recursively.
          // Enhancement: optional function call param for this.
          for (Long id : (List<Long>)obj.get("depend")) {
            JSONObject pk = (JSONObject) pkgs.get(id.intValue());
            download(pk.get("package").toString(), install);
          }
          return;
        }
      }
    }
  }

  /**
   * Determines status of some jar file.
   */
  private static String getStatus(String jarFile) throws SQLException {
    String fn = "${FARRAGO_HOME}/plugin/" + jarFile;
    String outfile = FarragoProperties.instance().expandProperties(fn);
    File f = new File(outfile);
    if (f.exists()) { // Either just downloaded or installed
      String ret = "DOWNLOADED";
      Connection conn = DriverManager.getConnection("jdbc:default:connection");
      String name = jarFile.replaceAll("\\.jar", "");
      String query = "SELECT name FROM localdb.sys_root.dba_jars WHERE " +
        "name = ? AND url IN (?,?)";
      PreparedStatement ps = conn.prepareStatement(query);
      ps.setString(1, name);
      ps.setString(2, fn);
      ps.setString(3, "file:" + fn);
      ResultSet rs = ps.executeQuery();
      if (rs.next()) {
        ret = "INSTALLED";
      }
      rs.close();
      ps.close();

      return ret;
    } else {
      return "AVAILABLE";
    }
  }

  /**
   * Provides the option to remove the jar associated with a package and their rdeps
   * from disk, lucidDB, or even both.
   */
  public static void remove(String pkgName, boolean fromDisk, boolean fromDB)
    throws SQLException
  {
    List<String> repos = getRepoUrls();
    for (String repo : repos) {
      JSONObject repo_data = downloadMetadata(repo);
      JSONArray pkgs = (JSONArray) repo_data.get("packages");
      for (JSONObject obj : (List<JSONObject>)pkgs) {
        if (pkgName.equals(obj.get("package").toString())) {
          if (fromDisk)
            removeJar(obj.get("jar").toString());
          if (fromDB)
            uninstallJar(obj.get("jar").toString());
          // automatically remove reverse-dependencies too, recursively.
          // Enhancement: optional function call param for this.
          for (Long id : (List<Long>)obj.get("rdepend")) {
            JSONObject pk = (JSONObject) pkgs.get(id.intValue());
            remove(pk.get("package").toString(), fromDisk, fromDB);
          }
          return;
        }
      }
    }
  }

  /**
   * Just installs the jar by file name, default in sys_network schema.
   */
  public static void installJar(String jarFile) throws SQLException {
    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    String name = jarFile.replaceAll("\\.jar", "");
    String query = "CREATE or REPLACE JAR localdb.sys_network.\"" + name + "\"\n" +
      "LIBRARY 'file:${FARRAGO_HOME}/plugin/" + jarFile + "'\n" +
      "OPTIONS(1)";
    PreparedStatement ps = conn.prepareStatement(
        "set schema 'localdb.sys_network'");
    ps.execute();
    ps = conn.prepareStatement(query);
    ps.execute();
    ps.close();
  }

  /**
   * Fetches and stores a .jar file from a repo url/file-url, doing an md5sum check too.
   */
  private static void fetchJar(String repo, String jarFile, String md5sum)
    throws SQLException
  {
    BufferedInputStream in = null;
    FileOutputStream out = null;
    try {
      String outfile = FarragoProperties.instance().expandProperties(
          "${FARRAGO_HOME}/plugin/" + jarFile);
      in = new BufferedInputStream(new URL(repo + "/" + jarFile).openStream());
      out = new FileOutputStream(outfile);
      final int block_size = 1 << 18; // 256 kb
      byte data[] = new byte[block_size];
      int bytes;
      // for verifying md5
      MessageDigest dig = MessageDigest.getInstance("MD5");
      while ((bytes = in.read(data, 0, block_size)) != -1) {
        out.write(data, 0, bytes);
        dig.update(data, 0, bytes);
      }
      in.close(); in = null;
      out.close(); out = null;

      java.math.BigInteger biggy = new java.math.BigInteger(1, dig.digest());
      String md5check = biggy.toString(16);
      if (!md5sum.equals(md5check)) {
        throw new SQLException(
            "Jar could not be fetched due to data mismatch.\n" +
            "Expected md5sum: " + md5sum + "; got " + md5check);
      }
    } catch (Throwable e) {
      throw new SQLException(e);
    } finally {
      try {
        if (in != null) in.close();
        if (out != null) out.close();
      } catch (IOException e) {
        //pass
      }
    }
  }

  /**
   * Just uninstalls a single .jar file without deletion.
   * Eats any errors.
   */
  public static void uninstallJar(String jarFile) {
    PreparedStatement ps = null;
    try {
      Connection conn = DriverManager.getConnection("jdbc:default:connection");
      String name = jarFile.replaceAll("\\.jar", "");
      String query = "DROP JAR localdb.sys_network.\"" + name + "\" OPTIONS(1) CASCADE";
      ps = conn.prepareStatement(query);
      ps.execute();
    } catch (Throwable e) {
      //munch
    } finally {
      try {
        if (ps != null) ps.close();
      } catch (SQLException e) {
        //pass
      }
    }
  }

  /**
   * Does the physical delete on the local file system of a single .jar file.
   * Eats any errors.
   */
  private static void removeJar(String jarFile) {
    try {
      String file = FarragoProperties.instance().expandProperties(
          "${FARRAGO_HOME}/plugin/" + jarFile);
      File f = new File(file);
      if (!f.delete())
        throw new SQLException("Deletion failed.");
    } catch (Throwable e) {
      //munch
    }
  }

  /**
   * Fetches the master metadata.json file on a given repository and returns the
   * data as a JSONObject.
   */
  public static JSONObject downloadMetadata(String repo) throws SQLException {
    if (repo == null)
      return null;

    try {
      // Grab master metadata.json
      URL u = new URL(repo + "/metadata.json");
      URLConnection uc = u.openConnection();
      uc.connect();
      InputStreamReader in = new InputStreamReader(uc.getInputStream());
      BufferedReader buff = new BufferedReader(in);
      StringBuffer sb = new StringBuffer();
      String line = null;
      do {
        line = buff.readLine();
        if (line != null) sb.append(line);
      } while (line != null);
      String data = sb.toString();

      // Parse it
      JSONParser parser = new JSONParser();
      JSONObject ob = (JSONObject) parser.parse(data);
      return ob;

    } catch (MalformedURLException e) {
      throw new SQLException("Bad URL.");
    } catch (IOException e) {
      throw new SQLException("Could not read data from URL.");
    } catch (ParseException e) {
      throw new SQLException("Could not parse data from URL.");
    }

  }

  /**
   * Grabbing the list of repos for internal use.
   */
  public static List<String> getRepoUrls() throws SQLException {
    List<String> urls = new ArrayList<String>();

    Connection conn = DriverManager.getConnection("jdbc:default:connection");
    String query = "SELECT repo_url FROM localdb.sys_network.repositories " +
      "ORDER BY repo_url";
    PreparedStatement ps = conn.prepareStatement(query);
    ps.execute();
    ResultSet rs = ps.getResultSet();
    while (rs.next()) {
      urls.add(rs.getString(1));
    }

    rs.close();
    ps.close();
    return urls;
  }

}
