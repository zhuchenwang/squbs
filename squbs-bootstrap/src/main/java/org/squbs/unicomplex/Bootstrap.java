package org.squbs.unicomplex;

import sun.net.www.ParseUtil;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhuwang on 2/26/15.
 */
public class Bootstrap {
  
  private final URLClassLoader cl;
  
  private static Bootstrap INSTANCE;
  
  public static Bootstrap getInstance() {
    if (INSTANCE == null) {
      try {
        INSTANCE = new Bootstrap();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return INSTANCE;
  }
  
  private Bootstrap() throws IOException {
    String squbsHome = System.getProperty("squbs.class.path");
    if (squbsHome == null) squbsHome = "lib";

    File squbsHomeDir = new File(squbsHome);
    String [] jarPaths = squbsHomeDir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar");
      }
    });
    if (jarPaths == null) {
      jarPaths = new String [0];
    }
    
    File [] jars = new File [jarPaths.length];
    Set<String> classpaths = new HashSet<>();
    for (int i = 0; i < jarPaths.length; i ++) {
      jars[i] = new File(squbsHomeDir, jarPaths[i]).getCanonicalFile();
      classpaths.add(jars[i].getAbsolutePath());
    }
    for (String classpath : System.getProperty("java.class.path").split(File.pathSeparator)) {
      classpaths.add(new File(classpath).getCanonicalFile().getAbsolutePath());
    }
    StringBuilder classpathStr = new StringBuilder();
    for (String classpath: classpaths) {
      classpathStr.append(File.pathSeparator);
      classpathStr.append(classpath);
    }
    System.setProperty("java.class.path", classpathStr.substring(1).toString());
    
    URL [] urls = new URL[jars.length];
    for (int i = 0; i < jars.length; i++) {
      try {
        urls[i] = ParseUtil.fileToEncodedURL(jars[i]);
      } catch (MalformedURLException e) {
        e.printStackTrace();
      }
    }

    cl = new URLClassLoader(urls, getClass().getClassLoader());
    Thread.currentThread().setContextClassLoader(cl);
  }
  
  public void start() throws IllegalAccessException, InstantiationException, ClassNotFoundException, 
      NoSuchMethodException, InvocationTargetException {
    Class bootstrapMain = cl.loadClass("org.squbs.unicomplex.BootstrapMain");
    final Method start = bootstrapMain.getMethod("start");
    start.invoke(bootstrapMain.newInstance());
  }
  
  public static void main(String [] args) {
    try {
      Bootstrap.getInstance().start();
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
  }
}
