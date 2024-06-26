USE [master]
GO

CREATE DATABASE [DapperWrapperTests];
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [DapperWrapperTests].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO

ALTER DATABASE [DapperWrapperTests] SET ANSI_NULL_DEFAULT OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET ANSI_NULLS ON 
GO

ALTER DATABASE [DapperWrapperTests] SET ANSI_PADDING ON 
GO

ALTER DATABASE [DapperWrapperTests] SET ANSI_WARNINGS ON 
GO

ALTER DATABASE [DapperWrapperTests] SET ARITHABORT ON 
GO

ALTER DATABASE [DapperWrapperTests] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET AUTO_SHRINK OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET AUTO_UPDATE_STATISTICS ON 
GO

ALTER DATABASE [DapperWrapperTests] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET CURSOR_DEFAULT  GLOBAL 
GO

ALTER DATABASE [DapperWrapperTests] SET CONCAT_NULL_YIELDS_NULL ON 
GO

ALTER DATABASE [DapperWrapperTests] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET QUOTED_IDENTIFIER ON 
GO

ALTER DATABASE [DapperWrapperTests] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET  DISABLE_BROKER 
GO

ALTER DATABASE [DapperWrapperTests] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET TRUSTWORTHY OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET PARAMETERIZATION SIMPLE 
GO

ALTER DATABASE [DapperWrapperTests] SET READ_COMMITTED_SNAPSHOT OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET HONOR_BROKER_PRIORITY OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET RECOVERY SIMPLE 
GO

ALTER DATABASE [DapperWrapperTests] SET  MULTI_USER 
GO

ALTER DATABASE [DapperWrapperTests] SET PAGE_VERIFY CHECKSUM  
GO

ALTER DATABASE [DapperWrapperTests] SET DB_CHAINING OFF 
GO

ALTER DATABASE [DapperWrapperTests] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO

ALTER DATABASE [DapperWrapperTests] SET TARGET_RECOVERY_TIME = 0 SECONDS 
GO

ALTER DATABASE [DapperWrapperTests] SET DELAYED_DURABILITY = DISABLED 
GO

ALTER DATABASE [DapperWrapperTests] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO

ALTER DATABASE [DapperWrapperTests] SET QUERY_STORE = OFF
GO


USE [master]
GO

ALTER DATABASE [DapperWrapperTests] SET  READ_WRITE 
GO