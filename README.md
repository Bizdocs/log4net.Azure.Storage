# Log4Net Azure Storage Appender

![.NET Core](https://github.com/VytenisJ/log4net.Azure.Storage/workflows/.NET%20Core/badge.svg)

Custom **Log4Net** appender to log to Azure Storage.

Currently available logging destination is **Blob Storage** as an **AppendBlob**

### Config example

```
<appender name="GeneralFileAppender" type="log4net.Azure.Storage.AzureBlobAppender, log4net.Azure.Storage">
    <BufferSize value="0" />
    <ContainerName value="logs"/>
    <DirectoryName value="rsl-fws"/>
    <DatePattern value="yyyy_MM_dd&quot;.log&quot;"/>
    <FileName value="info_" />

    <!-- You can either specify a connection string or use the ConnectionStringName property instead -->

    <!--<ConnectionString value="your_connection_string" />-->
    <ConnectionStringName value="your_connection_string_name" />

    <filter type="log4net.Filter.LevelRangeFilter">
      <levelMax value="INFO" />
    </filter>

    <layout type="log4net.Layout.PatternLayout">
      <conversionPattern value="%-5level %date{dd-MM-yyyy HH:mm:ss} %logger [%thread] - %message %exception %newline" />
    </layout>
</appender>
```

