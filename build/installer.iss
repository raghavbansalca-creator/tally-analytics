; Seven Labs Vision - Inno Setup Installer Script

[Setup]
AppName=Seven Labs Vision - Tally Analytics
AppVersion=1.0.0
AppPublisher=Seven Labs Vision
AppPublisherURL=https://github.com/raghavbansalca-creator/tally-analytics
DefaultDirName={localappdata}\SevenLabsVision
DefaultGroupName=Seven Labs Vision
OutputDir=dist
OutputBaseFilename=SevenLabsVision_Setup
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
DisableProgramGroupPage=yes

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "dist\TallyAnalytics\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{userdesktop}\Seven Labs Vision - Tally Analytics"; Filename: "{app}\SevenLabsVision.bat"; WorkingDir: "{app}"
Name: "{userstartmenu}\Seven Labs Vision"; Filename: "{app}\SevenLabsVision.bat"; WorkingDir: "{app}"

[Run]
Filename: "{app}\SevenLabsVision.bat"; Description: "Launch Seven Labs Vision"; Flags: nowait postinstall skipifsilent shellexec
